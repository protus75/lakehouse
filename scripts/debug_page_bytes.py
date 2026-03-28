"""Debug: show bytes around a search term on a specific page."""
import subprocess
import sys

CONTAINER = "lakehouse-workspace"
page = sys.argv[1] if len(sys.argv) > 1 else "120"
search = sys.argv[2] if len(sys.argv) > 2 else "Armor"

py_code = f"""
import sys
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
c = get_reader(["bronze_tabletop"])
rows = c.execute("SELECT page_text FROM bronze_tabletop.page_texts WHERE printed_page_num = ?", [{page}]).fetchall()
text = rows[0][0]
idx = text.find("{search}")
if idx < 0:
    # Try case insensitive
    idx = text.lower().find("{search}".lower())
if idx >= 0:
    start = max(0, idx - 60)
    end = min(len(text), idx + 60)
    chunk = text[start:end]
    print(f"Found at position {{idx}}")
    print(f"Context: {{repr(chunk)}}")
    print(f"Bytes: {{chunk.encode('utf-8')}}")
else:
    print(f"'{search}' not found on page {page}")
    print(f"First 200 bytes: {{repr(text[:200])}}")
"""

result = subprocess.run(
    ["docker", "exec", CONTAINER, "python", "-c", py_code],
    capture_output=True, text=True,
)
if result.returncode != 0:
    print(f"Error: {result.stderr}", file=sys.stderr)
print(result.stdout)
