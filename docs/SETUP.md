# Setup Guide

## Prerequisites

- **Databricks workspace** with Unity Catalog
- **Databricks CLI** v0.250+ ([install guide](https://docs.databricks.com/en/dev-tools/cli/index.html))
- **Python** 3.9+

## 1. Credentials

```bash
cp env.example .env
```

Edit `.env`:

```
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

Create a token at: **Workspace → Settings → Developer → Access tokens**

## 2. Unity Catalog

Create catalog, schema, and volume:

```sql
-- Use existing catalog or create one
CREATE SCHEMA IF NOT EXISTS <catalog>.`tko-project`;

CREATE VOLUME IF NOT EXISTS <catalog>.`tko-project`.raw_data;
```

Or via Databricks UI: **Data → Volumes → Create Volume**

Update `databricks.yml` variables:

- `catalog_name`: your catalog
- `schema_name`: `tko-project`
- `volume_name`: `raw_data`

Update `workspace.host` to your workspace URL.

## 3. Generate Mock Data

```bash
python scripts/generate_mock_data.py
```

Output: `data/raw/products_catalog.csv`, `customer_profiles.csv`, `clickstream_events.csv`, `clickstream_events.jsonl`

## 4. Upload to Volume

```bash
chmod +x scripts/upload_to_volume.sh
./scripts/upload_to_volume.sh
```

## 5. Verify

```bash
python scripts/test_workspace_connection.py
python scripts/test_volume_access.py
```

## 6. Deploy

```bash
./deploy.sh
```

## 7. Run Orchestration

```bash
# Load credentials first (e.g. source .env or set DATABRICKS_HOST, DATABRICKS_TOKEN)
./deploy.sh   # validates credentials
databricks bundle run tko_lakeflow_orchestration -t dev
```

This runs the DLT pipeline, then provisions Vector Search (endpoint + index).
