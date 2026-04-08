"""Probe: run PyMuPDF page.find_tables() on PHB and dump bbox + extracted rows.

Used to evaluate detection quality before committing to the table-extraction rewrite.
Compares detected regions per printed page against ToC `is_table` count from bronze.

Usage:
    python scripts/probe_pymupdf_tables.py                  # all pages, summary
    python scripts/probe_pymupdf_tables.py --pages 39,76,75 # specific printed pages, dump rows
    python scripts/probe_pymupdf_tables.py --dump           # dump rows for every detected table
"""
import subprocess
import sys

CONTAINER = "lakehouse-workspace"
PDF_PATH = "/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf"

pages_arg = ""
dump = False
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--pages" and i + 1 < len(sys.argv):
        pages_arg = sys.argv[i + 1]
        i += 2
    elif sys.argv[i] == "--dump":
        dump = True
        i += 1
    else:
        i += 1

py_code = f"""
import sys, json
sys.path.insert(0, "/workspace")
import fitz
from dlt.lib.duckdb_reader import get_reader

PDF = {PDF_PATH!r}
PAGES_ARG = {pages_arg!r}
DUMP = {dump!r}

# Load ToC is_table counts per printed page from bronze
toc_counts = {{}}
toc_titles = {{}}
try:
    c = get_reader(["bronze_tabletop"])
    rows = c.execute(
        "SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts ORDER BY page_index"
    ).fetchall()
    page_idx_to_printed = {{r[1]: r[0] for r in rows}}
    printed_to_page_idx = {{r[0]: r[1] for r in rows}}

    toc_rows = c.execute(
        "SELECT page_start, title FROM bronze_tabletop.toc_raw "
        "WHERE is_table = TRUE AND is_excluded = FALSE"
    ).fetchall()
    for p, t in toc_rows:
        toc_counts[p] = toc_counts.get(p, 0) + 1
        toc_titles.setdefault(p, []).append(t)
except Exception as e:
    print(f"WARN: could not load bronze ToC ({{e}}) — proceeding without comparison")
    page_idx_to_printed = {{}}
    printed_to_page_idx = {{}}

doc = fitz.open(PDF)
print(f"PDF: {{PDF}}")
print(f"Pages: {{len(doc)}}")
print(f"PyMuPDF: {{fitz.__version__}}")
print()

# Decide which page indices to scan
if PAGES_ARG:
    target_printed = [int(p) for p in PAGES_ARG.split(",")]
    target_indices = []
    for tp in target_printed:
        if tp in printed_to_page_idx:
            target_indices.append(printed_to_page_idx[tp])
        else:
            print(f"WARN: printed page {{tp}} not in bronze page_texts — skipping")
else:
    target_indices = list(range(len(doc)))

# Scan
detected = {{}}  # printed_page -> list of tables
total_regions = 0
errors = []
for page_idx in target_indices:
    page = doc[page_idx]
    printed = page_idx_to_printed.get(page_idx, page_idx)
    try:
        tabs = page.find_tables()
        tlist = list(tabs.tables) if hasattr(tabs, "tables") else list(tabs)
    except Exception as e:
        errors.append((page_idx, str(e)))
        continue
    if not tlist:
        continue
    detected[printed] = []
    for ti, t in enumerate(tlist):
        try:
            extracted = t.extract()
            n_rows = len(extracted)
            n_cols = max((len(r) for r in extracted), default=0)
        except Exception as e:
            extracted = None
            n_rows = n_cols = 0
        bbox = tuple(round(x, 1) for x in t.bbox)
        detected[printed].append({{
            "region_index": ti, "bbox": bbox,
            "rows": n_rows, "cols": n_cols,
            "extracted": extracted,
        }})
        total_regions += 1

doc.close()

# Per-page summary table
print(f"=== Per-page detection vs ToC is_table ===")
all_pages = sorted(set(detected.keys()) | set(toc_counts.keys()))
mismatches = 0
extra = 0
missing = 0
matched = 0
for pp in all_pages:
    det = len(detected.get(pp, []))
    toc = toc_counts.get(pp, 0)
    if det == toc and det > 0:
        matched += 1
        marker = "OK"
    elif det > toc:
        extra += 1
        mismatches += 1
        marker = "EXTRA"
    elif det < toc:
        missing += 1
        mismatches += 1
        marker = "MISSING"
    else:
        marker = "OK"
    if det or toc:
        titles = toc_titles.get(pp, [])
        title_hint = " | " + "; ".join(t[:30] for t in titles[:2]) if titles else ""
        print(f"  page {{pp:4}}: detected={{det}}  toc_is_table={{toc}}  [{{marker}}]{{title_hint}}")

print()
print(f"Totals: pages_with_tables={{len(detected)}}  total_regions={{total_regions}}")
print(f"  matched_pages={{matched}}  extra_pages={{extra}}  missing_pages={{missing}}")
print(f"  errors={{len(errors)}}")
if errors:
    for pi, e in errors[:5]:
        print(f"    page_idx {{pi}}: {{e}}")

if DUMP or PAGES_ARG:
    print()
    print(f"=== Detected table contents ===")
    for pp in sorted(detected.keys()):
        for tbl in detected[pp]:
            print(f"--- printed page {{pp}}, region {{tbl['region_index']}} ---")
            print(f"    bbox={{tbl['bbox']}}  rows={{tbl['rows']}}  cols={{tbl['cols']}}")
            if tbl["extracted"]:
                for ri, row in enumerate(tbl["extracted"][:8]):
                    cells = [str(c)[:30] if c is not None else "" for c in row]
                    print(f"    row{{ri}}: " + " | ".join(cells))
                if len(tbl["extracted"]) > 8:
                    print(f"    ... ({{len(tbl['extracted']) - 8}} more rows)")
"""

result = subprocess.run(
    ["docker", "exec", CONTAINER, "python", "-c", py_code],
    capture_output=True, text=True, encoding="utf-8",
)
sys.stdout.write(result.stdout)
if result.returncode != 0:
    sys.stderr.write(result.stderr)
    sys.exit(result.returncode)
