# Databricks notebook source
# MAGIC %md
# MAGIC # TKO Loyalty — Lakeflow Spark Declarative Pipeline
# MAGIC Medallion architecture: Bronze (ingest) → Silver (enrich) → Gold (aggregate).
# MAGIC
# MAGIC Ingests clickstream events and products catalog from a Unity Catalog Volume,
# MAGIC joins them, and computes per-customer **Category Interest** scores.

# COMMAND ----------

import dlt
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, when, sum as _sum, count, max as _max, rank, lit
)
from pyspark.sql.window import Window

# Runtime-overridable via pipeline configuration
VOLUME_PATH = spark.conf.get(
    "tko.volume_path",
    "/Volumes/classic_stable_1zia5t_kp_catalog/tko-project/raw_data",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

# Explicit schema — avoids inference overhead and acts as a data contract
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("timestamp", StringType()),
    StructField("duration_seconds", IntegerType()),
])


@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested via Auto Loader from the Unity Catalog Volume.",
    table_properties={"pipelines.reset.allowed": "true"},
)
def bronze_clickstream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(CLICKSTREAM_SCHEMA)
        .option("pathGlobFilter", "clickstream_events*.csv")
        .load(f"{VOLUME_PATH}/")
    )


@dlt.table(
    name="bronze_products",
    comment="Products catalog — static reference data.",
)
def bronze_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/products_catalog.csv")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------


@dlt.table(
    name="silver_clickstream_enriched",
    comment="Clickstream events enriched with product details.",
    table_properties={"pipelines.reset.allowed": "true"},
)
def silver_clickstream_enriched():
    clickstream = dlt.read_stream("bronze_clickstream")
    products = dlt.read("bronze_products")

    return (
        clickstream.join(products, on="product_id", how="left")
        .select(
            clickstream["event_id"],
            clickstream["customer_id"],
            clickstream["event_type"],
            clickstream["product_id"],
            products["product_name"],
            clickstream["category"],
            products["subcategory"],
            products["brand"],
            products["price"],
            col("timestamp").cast("timestamp").alias("event_timestamp"),
            clickstream["duration_seconds"],
            clickstream["session_id"],
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------


@dlt.table(
    name="gold_category_interest",
    comment="Per-customer category interest scores with weighted intent scoring.",
)
def gold_category_interest():
    enriched = dlt.read("silver_clickstream_enriched")

    # Weighted intent score per event type
    intent_score = (
        when(col("event_type") == "add_to_cart", lit(5))
        .when(col("event_type") == "wishlist", lit(4))
        .when(col("event_type") == "search", lit(3))
        .when(col("event_type") == "click", lit(2))
        .when(col("event_type") == "view", lit(1) + col("duration_seconds") / lit(60))
        .otherwise(lit(1))
    )

    scored = enriched.withColumn("intent_score", intent_score)

    aggregated = scored.groupBy("customer_id", "category").agg(
        _sum("intent_score").alias("interest_score"),
        count("*").alias("event_count"),
        _max("event_timestamp").alias("last_interaction"),
    )

    window = Window.partitionBy("customer_id").orderBy(col("interest_score").desc())

    return aggregated.withColumn("rank", rank().over(window))
