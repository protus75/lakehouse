#!/usr/bin/env python3
"""Query Iceberg tables via the workspace container.

Usage:
    python scripts/query_iceberg.py <namespace> <table> [--limit N] [--columns col1,col2]
    python scripts/query_iceberg.py bronze_tabletop ocr_issues --limit 20
    python scripts/query_iceberg.py bronze_tabletop ocr_issues --columns wrong_text,suggested_fix,status
"""

import json
import subprocess
import sys

CONTAINER = "lakehouse-workspace"


def query(namespace: str, table: str, limit: int = 0, columns: list[str] | None = None):
    cols_arg = repr(columns) if columns else "None"
    py_code = f"""
import json
from dlt.lib.iceberg_catalog import read_iceberg
t = read_iceberg("{namespace}", "{table}")
cols = {cols_arg}
if cols:
    t = t.select(cols)
rows = t.to_pydict()
n = len(list(rows.values())[0]) if rows else 0
print(f"Total rows: {{n}}")
limit = {limit}
if limit and limit < n:
    rows = {{k: v[:limit] for k, v in rows.items()}}
    print(f"Showing first {{limit}}")
for i in range(len(list(rows.values())[0])):
    row = {{k: str(v[i])[:200] for k, v in rows.items()}}
    print(json.dumps(row))
"""
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "python", "-c", py_code],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(result.stdout)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__)
        sys.exit(0)

    namespace = args[0]
    table = args[1]
    limit = 0
    columns = None

    i = 2
    while i < len(args):
        if args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--columns" and i + 1 < len(args):
            columns = args[i + 1].split(",")
            i += 2
        else:
            i += 1

    query(namespace, table, limit, columns)
