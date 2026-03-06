# Databricks notebook source
# MAGIC %md
# MAGIC # TKO Loyalty — Vector Search Setup
# MAGIC One-time setup notebook. Loads customer profiles into Unity Catalog,
# MAGIC creates `gold_product_catalog` with a description column suitable for
# MAGIC embedding, provisions a Vector Search endpoint, and creates a delta-sync
# MAGIC index for product similarity search.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "classic_stable_1zia5t_kp_catalog"
SCHEMA = "`tko-project`"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/tko-project/raw_data"

# Vector Search API requires alphanumerics/underscores only (no hyphens or backticks)
VS_SCHEMA = "tko_project"
VS_FULL_SCHEMA = f"{CATALOG}.{VS_SCHEMA}"
VS_ENDPOINT_NAME = "tko_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{VS_SCHEMA}.gold_product_catalog_index"

print(f"Catalog:      {CATALOG}")
print(f"Schema:       {SCHEMA}")
print(f"VS Schema:    {VS_SCHEMA}")
print(f"Volume:       {VOLUME_PATH}")
print(f"VS index:     {VS_INDEX_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Load `customer_profiles.csv` as UC table

# COMMAND ----------

# Create VS-compatible schema (underscores only) for Vector Search artifacts
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {VS_FULL_SCHEMA}")

df_profiles = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{VOLUME_PATH}/customer_profiles.csv")
)

df_profiles.write.mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.bronze_customer_profiles")

row_count = spark.table(f"{FULL_SCHEMA}.bronze_customer_profiles").count()
print(f"bronze_customer_profiles: {row_count} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Create `gold_product_catalog` with description column

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit, format_number

df_products = spark.table(f"{FULL_SCHEMA}.bronze_products")

# Build a natural-language description for embedding
df_catalog = df_products.withColumn(
    "description",
    concat_ws(
        ". ",
        concat_ws(" ", col("brand"), col("subcategory"), col("category")),
        concat_ws(" ", lit("Category:"), col("category")),
        concat_ws(" ", lit("Subcategory:"), col("subcategory")),
        concat_ws(" ", lit("Brand:"), col("brand")),
        concat_ws("", lit("Price: $"), format_number(col("price"), 2)),
    ),
)

# Write to VS-compatible schema (underscores) so Vector Search can reference it
(
    df_catalog.write
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{VS_FULL_SCHEMA}.gold_product_catalog")
)

# Verify CDF is enabled (required for delta-sync)
spark.sql(
    f"ALTER TABLE {VS_FULL_SCHEMA}.gold_product_catalog "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

display(spark.table(f"{VS_FULL_SCHEMA}.gold_product_catalog").limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Create Vector Search endpoint

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import time

vsc = VectorSearchClient()

# Create endpoint (no-op if it already exists)
try:
    vsc.create_endpoint(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Creating endpoint '{VS_ENDPOINT_NAME}' ...")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Endpoint '{VS_ENDPOINT_NAME}' already exists.")
    else:
        raise

# Poll until ONLINE
for i in range(60):
    ep = vsc.get_endpoint(VS_ENDPOINT_NAME)
    status = ep.get("endpoint_status", {}).get("state", "UNKNOWN")
    print(f"  [{i * 10}s] Endpoint status: {status}")
    if status == "ONLINE":
        break
    time.sleep(10)
else:
    print("WARNING: Endpoint did not reach ONLINE within 10 minutes.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Create delta-sync vector index

# COMMAND ----------

SOURCE_TABLE = f"{CATALOG}.{VS_SCHEMA}.gold_product_catalog"

try:
    index = vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        source_table_name=SOURCE_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="product_id",
        embedding_source_column="description",
        embedding_model_endpoint_name="databricks-gte-large-en",
        columns_to_sync=[
            "product_id", "product_name", "category", "subcategory",
            "brand", "price", "description",
        ],
    )
    print(f"Creating index '{VS_INDEX_NAME}' ...")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Index '{VS_INDEX_NAME}' already exists.")
        index = vsc.get_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME,
        )
    else:
        raise

# Poll until ready (graceful — transient 503s from VS control plane are common)
index_ready = False
for i in range(90):
    try:
        idx_status = index.describe()
        state = idx_status.get("status", {}).get("detailed_state", "UNKNOWN")
        ready = idx_status.get("status", {}).get("ready", False)
        print(f"  [{i * 10}s] Index state: {state} | ready: {ready}")
        if ready:
            index_ready = True
            break
    except Exception as e:
        print(f"  [{i * 10}s] Polling error (will retry): {e}")
    time.sleep(10)

if not index_ready:
    print("WARNING: Index not confirmed ready within 15 minutes. It may still be provisioning.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 — Smoke test

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)

try:
    results = index.similarity_search(
        query_text="casual denim jeans",
        columns=["product_id", "product_name", "category", "subcategory", "brand", "price"],
        num_results=5,
    )

    print("Top 5 results for 'casual denim jeans':")
    for row in results.get("result", {}).get("data_array", []):
        print(f"  {row}")
except Exception as e:
    print(f"Smoke test skipped — index may still be syncing: {e}")
    print("Re-run the smoke test manually once the index status shows 'ready'.")

# COMMAND ----------
