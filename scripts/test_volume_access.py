#!/usr/bin/env python3
"""
Verify access to TKO Unity Catalog schema and volume.
Uses databricks CLI (must have .env loaded).
Schema: classic_stable_1zia5t_kp_catalog.tko-project
Volume: /Volumes/classic_stable_1zia5t_kp_catalog/tko-project/raw_data
"""

import os
import subprocess
import sys

from _env import load_env

load_env()

if not os.getenv("DATABRICKS_TOKEN"):
    print("Error: Set DATABRICKS_TOKEN in .env", file=sys.stderr)
    sys.exit(1)
if os.getenv("DATABRICKS_HOST") and not os.getenv("DATABRICKS_HOST", "").startswith("https://"):
    os.environ["DATABRICKS_HOST"] = "https://" + os.getenv("DATABRICKS_HOST", "")

CATALOG = "classic_stable_1zia5t_kp_catalog"
SCHEMA = "tko-project"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"


def run(cmd: list) -> bool:
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode == 0, r.stdout, r.stderr


def main():
    print("TKO Unity Catalog Access Check")
    print("=" * 50)
    print(f"  Catalog: {CATALOG}")
    print(f"  Schema:  {SCHEMA}")
    print(f"  Volume:  {VOLUME_PATH}")
    print()

    ok, out, err = run(["databricks", "volumes", "list", CATALOG, SCHEMA])
    if not ok:
        print(f"✗ Volumes list failed: {err}")
        sys.exit(1)
    print("1. ✓ Schema & volume access OK")

    ok, out, err = run(["databricks", "fs", "ls", VOLUME_PATH])
    if not ok:
        print(f"✗ Volume path access failed: {err}")
        sys.exit(1)
    files = [f.strip() for f in out.strip().splitlines() if f.strip()]
    print(f"2. ✓ Volume path OK ({len(files)} file(s))")
    if files:
        for f in files:
            print(f"     - {f}")

    print()
    print("Access verified. Upload data: ./scripts/upload_to_volume.sh")
    sys.exit(0)


if __name__ == "__main__":
    main()
