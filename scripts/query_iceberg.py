#!/usr/bin/env python3
"""Query Iceberg tables via the workspace container.

Usage:
    python scripts/query_iceberg.py <namespace> <table> [options]
    python scripts/query_iceberg.py bronze_tabletop ocr_issues --limit 20
    python scripts/query_iceberg.py bronze_tabletop ocr_issues --columns wrong_text,suggested_fix
    python scripts/query_iceberg.py gold_tabletop gold_entries --where "entry_title = 'Fighter'" --full
    python scripts/query_iceberg.py gold_tabletop gold_entry_index --where "entry_type = 'table'" --columns entry_title,entry_type

Options:
    --limit N        Max rows to return (0 = all)
    --columns c1,c2  Only return these columns
    --where EXPR     SQL WHERE clause (applied via DuckDB over Arrow)
    --full           Don't truncate long values (default: 200 chars)
    --truncate N     Truncate values to N chars (default: 200, 0 = no truncate)
"""

import json
import subprocess
import sys
import os

CONTAINER = "lakehouse-workspace"


def query(namespace: str, table: str, limit: int = 0,
          columns: list[str] | None = None, where: str = "",
          truncate: int = 200):
    cols_arg = repr(columns) if columns else "None"
    where_escaped = where.replace('"', '\\"')
    py_code = f"""
import json, sys
sys.path.insert(0, "/workspace")
from dlt.lib.iceberg_catalog import read_iceberg
t = read_iceberg("{namespace}", "{table}")
cols = {cols_arg}
if cols:
    t = t.select(cols)

where_clause = "{where_escaped}"
if where_clause:
    import duckdb
    con = duckdb.connect()
    con.register("_tbl", t)
    t = con.execute(f"SELECT * FROM _tbl WHERE {{where_clause}}").fetch_arrow_table()

rows = t.to_pydict()
n = len(list(rows.values())[0]) if rows else 0
print(f"Total rows: {{n}}")
limit = {limit}
if limit and limit < n:
    rows = {{k: v[:limit] for k, v in rows.items()}}
    print(f"Showing first {{limit}}")
truncate = {truncate}
for i in range(len(list(rows.values())[0])):
    if truncate > 0:
        row = {{k: str(v[i])[:truncate] for k, v in rows.items()}}
    else:
        row = {{k: str(v[i]) for k, v in rows.items()}}
    print(json.dumps(row, ensure_ascii=False))
"""
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "python", "-c", py_code],
        capture_output=True, text=True, env=env, encoding="utf-8",
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    sys.stdout.buffer.write(result.stdout.encode("utf-8", errors="replace"))


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__)
        sys.exit(0)

    namespace = args[0]
    table = args[1]
    limit = 0
    columns = None
    where = ""
    truncate = 200

    i = 2
    while i < len(args):
        if args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--columns" and i + 1 < len(args):
            columns = args[i + 1].split(",")
            i += 2
        elif args[i] == "--where" and i + 1 < len(args):
            where = args[i + 1]
            i += 2
        elif args[i] == "--full":
            truncate = 0
            i += 1
        elif args[i] == "--truncate" and i + 1 < len(args):
            truncate = int(args[i + 1])
            i += 2
        else:
            i += 1

    query(namespace, table, limit, columns, where, truncate)
