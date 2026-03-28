"""Show page text for specific pages, searching for title strings.

Usage: python scripts/query_pages.py 49 72 120 141
"""
import subprocess
import sys

CONTAINER = "lakehouse-workspace"

pages = sys.argv[1:] if len(sys.argv) > 1 else ["49", "72", "120", "141"]
titles = [
    "Spells Allowed", "Spheres of Influence",
    "Related Weapon Bonus",
    "Various Types of Weapons", "The Various Types",
    "Herbalism and Healing",
]

py_code = f"""
import sys, os
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
os.makedirs("/workspace/output", exist_ok=True)
c = get_reader(["bronze_tabletop"])
pages = {pages}
with open("/workspace/output/page_dump.txt", "w", encoding="utf-8") as f:
    for pn in pages:
        rows = c.execute("SELECT page_text FROM bronze_tabletop.page_texts WHERE printed_page_num = ?", [int(pn)]).fetchall()
        if not rows:
            continue
        text = rows[0][0]
        f.write(f"=== Page {{pn}} ({{len(text)}} chars) ===\\n")
        f.write(text)
        f.write("\\n\\n")
print(f"Written {{len(pages)}} pages to output/page_dump.txt")
"""

result = subprocess.run(
    ["docker", "exec", CONTAINER, "python", "-c", py_code],
    capture_output=True, text=True,
)
if result.returncode != 0:
    print(f"Error: {result.stderr}", file=sys.stderr)
    sys.exit(1)
print(result.stdout)
