# Databricks notebook source
# MAGIC %md
# MAGIC # TKO Loyalty — Style Assistant Agent
# MAGIC RAG-powered agent that uses Vector Search to find products matching the
# MAGIC "vibe" of a shopper's recent search terms. Queries Unity Catalog tables
# MAGIC for customer context (profile, category interests, recent clickstream)
# MAGIC and generates personalized recommendations via Foundation Model APIs.
# MAGIC
# MAGIC Implements `mlflow.pyfunc.ChatModel` for compatibility with Databricks
# MAGIC Model Serving chat endpoints.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "classic_stable_1zia5t_kp_catalog"
SCHEMA = "`tko-project`"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

VS_ENDPOINT_NAME = "tko-vs-endpoint"
VS_INDEX_NAME = f"{CATALOG}.`tko-project`.gold_product_catalog_index"

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

print(f"Schema:       {FULL_SCHEMA}")
print(f"VS endpoint:  {VS_ENDPOINT_NAME}")
print(f"VS index:     {VS_INDEX_NAME}")
print(f"LLM:          {LLM_ENDPOINT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

import re
import mlflow
from mlflow.types.llm import (
    ChatMessage,
    ChatParams,
    ChatResponse,
    ChatChoice,
)
from databricks.vector_search.client import VectorSearchClient


def extract_customer_id(message: str) -> str | None:
    """Extract customer ID (c_XXXXX) from a message string."""
    match = re.search(r"c_\d{5}", message)
    return match.group(0) if match else None


def get_customer_profile(spark, customer_id: str) -> dict | None:
    """Fetch customer profile from bronze_customer_profiles."""
    rows = spark.sql(f"""
        SELECT customer_id, first_name, last_name, email,
               loyalty_tier, total_purchases, ltv, signup_date, last_purchase_date
        FROM {FULL_SCHEMA}.bronze_customer_profiles
        WHERE customer_id = '{customer_id}'
    """).collect()
    if not rows:
        return None
    r = rows[0]
    return r.asDict()


def get_top_categories(spark, customer_id: str, top_n: int = 5) -> list[dict]:
    """Get top category interests from gold_category_interest."""
    rows = spark.sql(f"""
        SELECT category, interest_score, event_count, last_interaction
        FROM {FULL_SCHEMA}.gold_category_interest
        WHERE customer_id = '{customer_id}'
        ORDER BY interest_score DESC
        LIMIT {top_n}
    """).collect()
    return [r.asDict() for r in rows]


def get_recent_activity(spark, customer_id: str, hours: int = 48) -> list[dict]:
    """Get recent clickstream activity from silver_clickstream_enriched."""
    rows = spark.sql(f"""
        SELECT event_type, product_id, product_name, category,
               subcategory, brand, price, event_timestamp
        FROM {FULL_SCHEMA}.silver_clickstream_enriched
        WHERE customer_id = '{customer_id}'
          AND event_timestamp >= current_timestamp() - INTERVAL {hours} HOURS
        ORDER BY event_timestamp DESC
        LIMIT 20
    """).collect()
    return [r.asDict() for r in rows]


def search_products(query_text: str, num_results: int = 10) -> list[dict]:
    """Similarity search against the gold_product_catalog Vector Search index."""
    vsc = VectorSearchClient()
    index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
    results = index.similarity_search(
        query_text=query_text,
        columns=[
            "product_id", "product_name", "category",
            "subcategory", "brand", "price",
        ],
        num_results=num_results,
    )
    data = results.get("result", {}).get("data_array", [])
    columns = ["product_id", "product_name", "category", "subcategory", "brand", "price", "score"]
    return [dict(zip(columns, row)) for row in data]


def build_vibe_query(
    user_message: str,
    categories: list[dict],
    activity: list[dict],
) -> str:
    """Combine user intent, top categories, and recent browsing into a search query."""
    parts = [user_message]

    if categories:
        top_cats = ", ".join(c["category"] for c in categories[:3])
        parts.append(f"Interests: {top_cats}")

    if activity:
        recent_brands = set()
        recent_subcats = set()
        for a in activity[:10]:
            if a.get("brand"):
                recent_brands.add(str(a["brand"]))
            if a.get("subcategory"):
                recent_subcats.add(str(a["subcategory"]))
        if recent_brands:
            parts.append(f"Brands: {', '.join(list(recent_brands)[:5])}")
        if recent_subcats:
            parts.append(f"Styles: {', '.join(list(recent_subcats)[:5])}")

    return ". ".join(parts)


def generate_recommendations(
    profile: dict | None,
    categories: list[dict],
    activity: list[dict],
    products: list[dict],
    user_query: str,
) -> str:
    """Call the Foundation Model API to produce personalized recommendations."""
    client = mlflow.deployments.get_deploy_client("databricks")

    # Build context sections
    profile_text = "No profile found."
    if profile:
        profile_text = (
            f"Name: {profile['first_name']} {profile['last_name']}\n"
            f"Loyalty tier: {profile['loyalty_tier']}\n"
            f"Total purchases: {profile['total_purchases']}\n"
            f"Lifetime value: ${profile['ltv']}\n"
            f"Last purchase: {profile['last_purchase_date']}"
        )

    categories_text = "No category data."
    if categories:
        lines = [
            f"- {c['category']}: score {c['interest_score']:.1f} ({c['event_count']} events)"
            for c in categories
        ]
        categories_text = "\n".join(lines)

    activity_text = "No recent activity."
    if activity:
        lines = [
            f"- {a['event_type']} {a['product_name']} ({a['brand']}, ${a['price']})"
            for a in activity[:10]
        ]
        activity_text = "\n".join(lines)

    products_text = "No matching products found."
    if products:
        lines = [
            f"- {p['product_name']} by {p['brand']} — ${p['price']} "
            f"({p['category']}/{p['subcategory']}, match: {p.get('score', 'N/A')})"
            for p in products
        ]
        products_text = "\n".join(lines)

    system_prompt = """You are the TKO Style Assistant, a personal shopping advisor for a fashion retail loyalty program.

Your job is to recommend 3-5 specific products from the MATCHING PRODUCTS list below. For each recommendation:
1. Name the exact product with its price
2. Explain WHY it suits this customer based on their profile, interests, and recent browsing
3. Be warm and personalized — address the customer by first name and reference their loyalty tier

If the customer is a higher-tier member (gold/platinum), mention exclusive perks or early access.
Keep it conversational and concise. Do not recommend products NOT in the matching products list.

CUSTOMER PROFILE:
{profile}

TOP CATEGORY INTERESTS:
{categories}

RECENT BROWSING (last 48h):
{activity}

MATCHING PRODUCTS (from vector search):
{products}""".format(
        profile=profile_text,
        categories=categories_text,
        activity=activity_text,
        products=products_text,
    )

    response = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query},
            ],
            "max_tokens": 1024,
            "temperature": 0.7,
        },
    )

    return response["choices"][0]["message"]["content"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## StyleAssistant ChatModel

# COMMAND ----------


class StyleAssistant(mlflow.pyfunc.ChatModel):
    """RAG-powered style recommendation agent backed by Vector Search."""

    def predict(self, context, messages: list[ChatMessage], params: ChatParams = None) -> ChatResponse:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        # Use the last user message
        user_msg = messages[-1].content if messages else ""

        # 1. Extract customer ID
        customer_id = extract_customer_id(user_msg)

        # 2. Gather context from UC tables
        profile = None
        categories = []
        activity = []

        if customer_id:
            profile = get_customer_profile(spark, customer_id)
            categories = get_top_categories(spark, customer_id)
            activity = get_recent_activity(spark, customer_id)

        # 3. Build vibe query and search products
        vibe_query = build_vibe_query(user_msg, categories, activity)
        products = search_products(vibe_query)

        # 4. Generate recommendations via LLM
        reply = generate_recommendations(
            profile=profile,
            categories=categories,
            activity=activity,
            products=products,
            user_query=user_msg,
        )

        return ChatResponse(
            choices=[
                ChatChoice(
                    index=0,
                    message=ChatMessage(role="assistant", content=reply),
                )
            ],
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Interactive Tests

# COMMAND ----------
# MAGIC %md
# MAGIC ### Test 1 — Customer with style query

# COMMAND ----------

agent = StyleAssistant()

test1 = agent.predict(
    context=None,
    messages=[ChatMessage(role="user", content="Customer c_00042 wants casual weekend outfit with denim")],
)

print("--- Test 1: Customer with style query ---")
print(test1.choices[0].message.content)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Test 2 — Customer ID only

# COMMAND ----------

test2 = agent.predict(
    context=None,
    messages=[ChatMessage(role="user", content="c_00010")],
)

print("--- Test 2: Customer ID only ---")
print(test2.choices[0].message.content)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Test 3 — Unknown customer

# COMMAND ----------

test3 = agent.predict(
    context=None,
    messages=[ChatMessage(role="user", content="c_99999 looking for shoes")],
)

print("--- Test 3: Unknown customer ---")
print(test3.choices[0].message.content)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Test 4 — No customer ID

# COMMAND ----------

test4 = agent.predict(
    context=None,
    messages=[ChatMessage(role="user", content="I want something trendy for summer")],
)

print("--- Test 4: No customer ID ---")
print(test4.choices[0].message.content)

# COMMAND ----------
# MAGIC %md
# MAGIC ## MLflow Model Logging

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="style-assistant-agent"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="style-assistant",
        python_model=StyleAssistant(),
        pip_requirements=[
            "mlflow>=2.12",
            "databricks-vectorsearch",
            "databricks-sdk",
        ],
        registered_model_name=f"{FULL_SCHEMA}.style_assistant",
    )

print(f"Model URI: {model_info.model_uri}")
print(f"Run ID:    {model_info.run_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify loaded model

# COMMAND ----------

loaded = mlflow.pyfunc.load_model(model_info.model_uri)

verify = loaded.predict(
    {"messages": [{"role": "user", "content": "c_00042 wants casual denim"}]}
)
print("--- Loaded model verification ---")
print(verify)

# COMMAND ----------
