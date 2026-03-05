#!/usr/bin/env bash
# Upload mock data from data/raw/ to Unity Catalog Volume
# Volume: /Volumes/classic_stable_1zia5t_kp_catalog/tko-project/raw_data

set -e
cd "$(dirname "$0")/.."

# Load .env
if [[ -f .env ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    export "$line"
  done < .env
  [[ -n "$DATABRICKS_HOST" && "$DATABRICKS_HOST" != https://* ]] && export DATABRICKS_HOST="https://${DATABRICKS_HOST}"
fi

VOLUME="dbfs:/Volumes/classic_stable_1zia5t_kp_catalog/tko-project/raw_data"
RAW="data/raw"

if [[ ! -d "$RAW" ]]; then
  echo "Run scripts/generate_mock_data.py first to create data."
  exit 1
fi

echo "Uploading to $VOLUME"
for f in products_catalog.csv customer_profiles.csv clickstream_events.csv clickstream_events.jsonl; do
  if [[ -f "$RAW/$f" ]]; then
    databricks fs cp "$RAW/$f" "$VOLUME/$f" --overwrite
    echo "  ✓ $f"
  fi
done
echo "Done."
