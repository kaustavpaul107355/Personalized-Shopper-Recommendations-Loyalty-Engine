# Databricks notebook source
# MAGIC %md
# MAGIC # TKO Loyalty App — Bootstrap
# MAGIC Deployed via Databricks Asset Bundle. Validates workspace connectivity and catalog access.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "local")
print(f"Workspace: {spark.conf.get('spark.databricks.workspaceUrl', 'N/A')}")
print("TKO bundle deployed successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Catalog check

# COMMAND ----------
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print(f"Catalogs: {[r['catalog'] for r in catalogs]}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------
