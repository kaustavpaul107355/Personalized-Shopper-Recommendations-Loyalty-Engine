#!/usr/bin/env python3
"""
Test connection to the Databricks workspace.
Reads credentials from .env and calls the REST API.
"""

import json
import os
import sys
import urllib.error
import urllib.request

from _env import load_env

load_env()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

if not host or not token:
    print("Error: Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env", file=sys.stderr)
    sys.exit(1)

# Ensure https
base_url = host if host.startswith("http") else f"https://{host}"

try:
    # Try clusters/list (widely supported) then SCIM Me as fallback
    for endpoint in ["/api/2.0/clusters/list", "/api/2.0/preview/scim/v2/Me"]:
        req = urllib.request.Request(
            f"{base_url}{endpoint}",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read().decode()
                body = json.loads(data)
                if "userName" in body:
                    name = body.get("userName") or body.get("displayName") or "authenticated"
                else:
                    name = f"{len(body.get('clusters', []))} clusters visible"
                print(f"✓ Connected to {base_url}")
                print(f"  Status: {name}")
                sys.exit(0)
        except urllib.error.HTTPError as he:
            if he.code == 404:
                continue
            if he.code == 401:
                print("✗ Auth failed: invalid or expired token", file=sys.stderr)
                sys.exit(1)
            raise
    print("✗ Connection failed: endpoints not found (404)", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"✗ Connection failed: {e}", file=sys.stderr)
    sys.exit(1)
