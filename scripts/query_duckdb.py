#!/usr/bin/env python3
"""Query dbt DuckDB tables via the workspace container.

Use this for silver/gold tables that dbt materializes to DuckDB.
For bronze tables on S3/Iceberg, use query_iceberg.py instead.

Usage:
    python scripts/query_duckdb.py <sql>
    python scripts/query_duckdb.py "SELECT * FROM silver_tabletop.silver_entries LIMIT 5"
    python scripts/query_duckdb.py "SELECT toc_id, title, count(*) FROM silver_tabletop.silver_toc_sections GROUP BY 1,2 HAVING count(*) > 1"
"""

import subprocess
import sys

CONTAINER = "lakehouse-workspace"
DB_PATH = "/workspace/db/lakehouse.duckdb"


def query(sql: str):
    py_code = f"""
import json, duckdb
con = duckdb.connect("{DB_PATH}", read_only=True)
try:
    rows = con.execute('''{sql}''').fetchall()
    cols = [d[0] for d in con.description]
    print(f"Columns: {{cols}}")
    print(f"Total rows: {{len(rows)}}")
    for row in rows:
        print(json.dumps(dict(zip(cols, [str(v)[:200] for v in row]))))
except Exception as e:
    print(f"Error: {{e}}")
finally:
    con.close()
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
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)
    query(sys.argv[1])
