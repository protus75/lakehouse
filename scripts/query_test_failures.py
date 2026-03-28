"""Query dbt test failures from Iceberg and print details."""
import sys
sys.path.insert(0, "/workspace")

from dlt.lib.duckdb_reader import get_reader

c = get_reader(["meta"])
rows = c.execute("""
    SELECT test_name, status, failures, message
    FROM meta.dbt_test_results
    WHERE status = 'fail'
    ORDER BY test_name
""").fetchall()

for name, status, failures, msg in rows:
    short = name.replace("test.lakehouse_mvp.", "")
    print(f"FAIL: {short} ({failures} failures)")
    print(f"  {msg}")
    print()
