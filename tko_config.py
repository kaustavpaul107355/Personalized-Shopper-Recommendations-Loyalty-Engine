"""
TKO Loyalty App — Unity Catalog configuration.
Use in notebooks and scripts for consistent paths.
"""

CATALOG = "classic_stable_1zia5t_kp_catalog"
SCHEMA = "tko-project"
VOLUME_NAME = "raw_data"

# Full paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
VOLUME_PATH_DBFS = f"dbfs:{VOLUME_PATH}"

# Table names (for future Silver/Gold)
TABLE_PRODUCTS = f"{CATALOG}.{SCHEMA}.products_catalog"
TABLE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.customer_profiles"
TABLE_CLICKSTREAM = f"{CATALOG}.{SCHEMA}.clickstream_events"
