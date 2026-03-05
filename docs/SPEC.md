# TKO Loyalty App — Specification

Full build scope and requirements for tracking.

---

## Business Challenges

- **Relevance Fatigue**: Generic marketing → lower CTR, brand erosion
- **Static Loyalty**: Points update daily; no real-time shopper intent
- **Conversion Gap**: Missed conversions; "perfect offer" arrives too late

## Databricks Value

- Unify shopper journey into **Single Source of Truth**
- GenAI as reasoning engine → match products to lifestyle
- AI pipelines: raw logs → actionable AI layer
- ~30% conversion increase potential

---

## Build Steps (Original)

1. **Simulate** — Mock datasets (CSV/JSON): customers, purchase history, clickstream → Unity Catalog Volume ✅
2. **Conversational BI** — Genie Space for marketers (planned)
3. **Evaluate** — Style Assistant agent (Vector Search + LLM) ✅
4. **Triage** — Lakebase for Live Recommendation Scores (planned)
5. **Serve** — Databricks App Customer Portal (planned)

---

## Customer Requirements

- **Instant Gratification**: Real-time recommendations from session behavior
- **Privacy First**: Unity Catalog permissions; no raw PII exposure
- **Scalability**: Millions of concurrent product lookups

---

## Technical Requirements

| Component | Status |
|-----------|--------|
| Unity Catalog (schema, volume, tags) | ✅ |
| Lakeflow Spark Declarative Pipelines | ✅ |
| Databricks Genie | Planned |
| Lakebase (State Store) | Planned |
| Model Serving (RAG + Vector Search) | ✅ |
| Databricks Connect / Cursor | ✅ |

---

## Implemented Architecture

- **Mock data**: Products (~200), customers (150), clickstream (5k events)
- **DLT**: Bronze → Silver → Gold; Auto Loader from Volume; intent scores
- **Vector Search**: Product catalog embeddings, delta-sync index
- **Style Assistant**: MLflow ChatModel, Vector Search retrieval, Llama 3 70B
- **Orchestration**: Job chains DLT + Vector Search setup
