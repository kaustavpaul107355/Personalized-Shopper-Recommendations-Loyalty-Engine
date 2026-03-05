#!/usr/bin/env bash
# Deploy TKO Loyalty App to Databricks via Asset Bundle
# Loads credentials from .env and runs bundle deploy.

set -e
cd "$(dirname "$0")"

# Load .env (export vars for Databricks CLI)
if [[ -f .env ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    export "$line"
  done < .env
  if [[ -n "$DATABRICKS_HOST" && "$DATABRICKS_HOST" != https://* ]]; then
    export DATABRICKS_HOST="https://${DATABRICKS_HOST}"
  fi
fi

if [[ -z "$DATABRICKS_TOKEN" ]]; then
  echo "Error: DATABRICKS_TOKEN not set. Copy env.example to .env and set credentials."
  exit 1
fi

TARGET="${1:-dev}"
echo "Deploying TKO bundle to target: $TARGET"
echo "Workspace: ${DATABRICKS_HOST:-https://fevm-classic-stable-1zia5t-kp.cloud.databricks.com}"
echo ""

databricks bundle validate -t "$TARGET"
databricks bundle deploy -t "$TARGET"

echo ""
echo "Done. Run the bootstrap job:"
echo "  databricks bundle run tko_bootstrap -t $TARGET"
