# TKO Hyper-Personalized Loyalty App

A Databricks-powered reference implementation for **hyper-personalized retail loyalty**. Uses mock apparel data, Lakeflow DLT pipelines, Vector Search, and a RAG-based Style Assistant agent to demonstrate real-time, AI-driven personalization.

---

## Problem

Retailers face:

- **Relevance fatigue** — Generic marketing leads to lower CTR and brand erosion
- **Static loyalty** — Points update once a day; no use of real-time browse intent
- **Conversion gap** — "Perfect offer" arrives too late; missed long-term value

## Solution

Databricks unifies the shopper journey into a single source of truth, with GenAI and pipelines turning raw logs into an actionable AI layer. This project shows how with:

- **Medallion DLT pipeline** (Bronze → Silver → Gold) for clickstream and products
- **Vector Search** for product similarity / semantic search
- **Style Assistant agent** (RAG + LLM) for personalized recommendations
- **Unity Catalog** for governed, volume-backed raw data

---

## Quick Start

### Prerequisites

- Databricks workspace (Unity Catalog enabled)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) v0.250+
- Python 3.9+

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd tko-project

cp env.example .env
# Edit .env: set DATABRICKS_HOST and DATABRICKS_TOKEN
```

### 2. Update workspace (if needed)

Edit `databricks.yml` and set `workspace.host` to your Databricks workspace URL.

Create schema and volume in Unity Catalog:

- Catalog: `<your_catalog>`
- Schema: `tko-project`
- Volume: `raw_data` (path: `/Volumes/<catalog>/tko-project/raw_data`)

See [docs/SETUP.md](docs/SETUP.md) for details.

### 3. Generate mock data

```bash
python scripts/generate_mock_data.py
```

### 4. Upload to Unity Catalog Volume

```bash
./scripts/upload_to_volume.sh
```

### 5. Verify and deploy

```bash
python scripts/test_workspace_connection.py
python scripts/test_volume_access.py

./deploy.sh
```

### 6. Run orchestration

```bash
./deploy.sh   # validates credentials
databricks bundle run tko_lakeflow_orchestration -t dev
```

---

## Project Structure

```
tko-project/
├── databricks.yml              # Asset bundle config
├── deploy.sh                   # Deploy script
├── tko_config.py               # Catalog/schema/volume constants
├── env.example                 # Credentials template
├── README.md
├── docs/
│   ├── SETUP.md               # Setup and prerequisites
│   └── SPEC.md                # Full specification
├── resources/
│   ├── jobs/
│   │   ├── tko_bootstrap.yml
│   │   └── tko_lakeflow_orchestration.yml
│   └── pipelines/
│       └── tko_dlt_pipeline.yml
├── src/
│   └── notebooks/
│       ├── tko_bootstrap.py
│       ├── tko_dlt_pipeline.py      # DLT medallion
│       ├── tko_setup_vector_search.py
│       └── tko_style_agent.py      # RAG recommendation agent
├── scripts/
│   ├── generate_mock_data.py
│   ├── upload_to_volume.sh
│   ├── test_workspace_connection.py
│   └── test_volume_access.py
└── data/
    └── raw/                    # Generated mock data (gitignored)
```

---

## What's Built

| Component | Description |
|-----------|-------------|
| **Mock data** | ~200 products, 150 customers, 5,000 clickstream events |
| **DLT pipeline** | Bronze (Auto Loader) → Silver (joins, enrichment) → Gold (intent scores) |
| **Vector Search** | Product catalog index for semantic similarity |
| **Style Assistant** | ChatModel agent using Vector Search + Llama 3 70B |
| **Orchestration job** | Runs DLT, then Vector Search setup |

---

## Tools Used

- **Databricks Asset Bundles** — Deployment
- **Unity Catalog** — Data governance, volumes
- **Lakeflow Spark Declarative Pipelines** — DLT medallion
- **Databricks Vector Search** — Product embeddings and search
- **MLflow** — ChatModel for Model Serving
- **Databricks Model Serving** — Llama 3 70B

---

## License

See repository license.
