"""Table extraction tooling — single entrypoint for all table-detection probes,
detection runs, inspection, and per-page debugging.

Subcommands:
    fonts <pages...>            Per-page font histogram + non-body span dump.
                                Used during per-book onboarding to identify
                                body style and table-header style.

    vlm <pages...>              Probe a vision model (minicpm-v) for table
                                bboxes on the given pages. Renders boxes onto
                                the page image so accuracy is eyeball-able.
                                NOTE: minicpm-v failed this probe on PHB —
                                kept for testing other VLMs.

    detect [--pages a,b,c]      Run dlt.lib.table_regions.detect_table_regions
                                across the book. Without --pages, scans all
                                pages and writes regions_summary.json. With
                                --pages, also renders per-page debug PNGs.

    inspect [STATUS|PAGE...]    Inspect regions_summary.json. With no args,
                                prints totals. With status names (EXTRA,
                                MISSING, UNDER, OK), lists pages of that
                                status. With page numbers, prints region
                                detail for those pages.

    debug PAGE                  Verbose dump of detect_table_regions internals
                                for one page: spans, header rows, clusters,
                                column ranges, and the all-rows trace window.

All file outputs land in verify_output/table_probe/.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

CONTAINER = "lakehouse-workspace"
PDF_PATH = "/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf"
CONFIG_PATH = "/workspace/documents/tabletop_rules/configs/DnD2e_Handbook_Player.yaml"
CONTAINER_OUT = "/workspace/verify_output/table_probe"
HOST_OUT = Path("verify_output/table_probe")
SUMMARY_JSON = HOST_OUT / "regions_summary.json"


def _run_in_container(py_code: str) -> str:
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "python", "-c", py_code],
        capture_output=True, text=True, encoding="utf-8",
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        sys.exit(result.returncode)
    return result.stdout


def _copy_container_outputs():
    """Copy /workspace/verify_output/table_probe/* from container to host."""
    HOST_OUT.mkdir(parents=True, exist_ok=True)
    import os as _os
    env = {**_os.environ, "MSYS_NO_PATHCONV": "1"}
    subprocess.run(
        ["docker", "cp",
         f"{CONTAINER}://workspace/verify_output/table_probe/.",
         str(HOST_OUT) + "/"],
        env=env, capture_output=True, text=True,
    )


# ── Subcommand: fonts ──────────────────────────────────────────────

def cmd_fonts(args):
    pages = args.pages or [19, 27, 76, 90, 94, 121]
    py = f"""
import sys, json, os
sys.path.insert(0, "/workspace")
import fitz
from collections import Counter

PDF = {PDF_PATH!r}
TARGET = {pages!r}
OUT_DIR = {CONTAINER_OUT!r}
os.makedirs(OUT_DIR, exist_ok=True)

from dlt.lib.duckdb_reader import get_reader
c = get_reader(["bronze_tabletop"])
rows = c.execute(
    "SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts"
).fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}

doc = fitz.open(PDF)
results = {{}}
for printed in TARGET:
    if printed not in printed_to_idx:
        results[printed] = {{"error": "not in bronze"}}
        continue
    page = doc[printed_to_idx[printed]]
    d = page.get_text("dict")
    hist = Counter()
    spans = []
    for block in d.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                font = span.get("font", "")
                size = round(span.get("size", 0), 1)
                bold = bool(span.get("flags", 0) & 16)
                key = (font, size, bold)
                hist[key] += len(span.get("text", ""))
                spans.append({{
                    "font": font, "size": size, "bold": bold,
                    "bbox": [round(x, 1) for x in span.get("bbox", [])],
                    "text": span.get("text", "")[:60],
                }})
    body_style = hist.most_common(1)[0][0] if hist else None
    body_chars = hist[body_style] if body_style else 0
    total_chars = sum(hist.values())
    non_body = [s for s in spans if (s["font"], s["size"], s["bold"]) != body_style]
    results[printed] = {{
        "page_idx": printed_to_idx[printed],
        "page_size": [round(page.rect.width, 1), round(page.rect.height, 1)],
        "body_style": list(body_style) if body_style else None,
        "body_pct": round(100 * body_chars / total_chars, 1) if total_chars else 0,
        "total_chars": total_chars,
        "histogram": [
            {{"font": k[0], "size": k[1], "bold": k[2], "chars": v,
              "pct": round(100 * v / total_chars, 1) if total_chars else 0}}
            for k, v in hist.most_common(10)
        ],
        "non_body_spans": non_body[:80],
    }}
doc.close()
print(json.dumps(results, ensure_ascii=False))
"""
    out = _run_in_container(py)
    data = json.loads(out)

    HOST_OUT.mkdir(parents=True, exist_ok=True)
    print("=== Font histogram probe ===")
    for printed_str, info in data.items():
        printed = int(printed_str)
        if "error" in info:
            print(f"  page {printed}: {info['error']}")
            continue
        print(f"  page {printed}: body={info['body_pct']}% non_body_spans={len(info['non_body_spans'])} body_style={info['body_style']}")

        lines = [f"=== Page {printed} (idx {info['page_idx']}, size {info['page_size']}) ==="]
        lines.append(f"Body style: {info['body_style']} ({info['body_pct']}%)")
        lines.append(f"Total chars: {info['total_chars']}")
        lines.append("")
        lines.append("Histogram (top 10):")
        for h in info["histogram"]:
            marker = "  BODY" if h["pct"] == info["body_pct"] else ""
            lines.append(f"  {h['chars']:6d} ({h['pct']:5.1f}%)  {h['font']:30s} {h['size']:5.1f}  bold={str(h['bold']):5s}{marker}")
        lines.append("")
        lines.append(f"Non-body spans ({len(info['non_body_spans'])}):")
        for s in info["non_body_spans"]:
            lines.append(f"  bbox={s['bbox']}  {s['font'][:25]:25s} {s['size']:4.1f} bold={str(s['bold']):5s}  {s['text']!r}")
        (HOST_OUT / f"fonts_page_{printed:03d}.txt").write_text("\n".join(lines), encoding="utf-8")
    print(f"\nDetail: {HOST_OUT}/fonts_page_*.txt")


# ── Subcommand: vlm ────────────────────────────────────────────────

def cmd_vlm(args):
    pages = args.pages or [19, 27, 76, 90, 94, 121]
    model = args.model
    py = f"""
import sys, json, base64, os, io
sys.path.insert(0, "/workspace")
import fitz, requests
from PIL import Image, ImageDraw

PDF = {PDF_PATH!r}
TARGET = {pages!r}
MODEL = {model!r}
OUT_DIR = {CONTAINER_OUT!r}
os.makedirs(OUT_DIR, exist_ok=True)

VLM_URL = "http://host.docker.internal:11434"
PROMPT = (
    "This is a page from a tabletop RPG rulebook. "
    "Identify every TABLE on this page (rows and columns of data, NOT prose). "
    "Return ONLY a JSON array of bounding boxes in normalized 0..1 coords: "
    '[{{"x0": 0.1, "y0": 0.2, "x1": 0.9, "y1": 0.4}}, ...]. '
    "Return [] if no tables."
)

from dlt.lib.duckdb_reader import get_reader
c = get_reader(["bronze_tabletop"])
rows = c.execute("SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts").fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}

doc = fitz.open(PDF)
results = {{}}
for printed in TARGET:
    if printed not in printed_to_idx:
        results[printed] = {{"error": "not in bronze"}}
        continue
    page = doc[printed_to_idx[printed]]
    pix = page.get_pixmap(dpi=200)
    img_bytes = pix.tobytes("png")
    with open(f"{{OUT_DIR}}/vlm_page_{{printed:03d}}_orig.png", "wb") as f:
        f.write(img_bytes)
    img_b64 = base64.b64encode(img_bytes).decode()
    try:
        resp = requests.post(
            f"{{VLM_URL}}/api/generate",
            json={{"model": MODEL, "prompt": PROMPT, "images": [img_b64],
                   "stream": False, "options": {{"temperature": 0.0, "num_predict": 1024}}}},
            timeout=180,
        )
        resp.raise_for_status()
        response_text = resp.json().get("response", "").strip()
    except Exception as e:
        results[printed] = {{"error": str(e)}}
        continue

    bboxes = []
    parse_err = None
    try:
        s = response_text.find("[")
        e = response_text.rfind("]") + 1
        if s >= 0 and e > s:
            parsed = json.loads(response_text[s:e])
            for item in parsed:
                if isinstance(item, dict) and all(k in item for k in ("x0", "y0", "x1", "y1")):
                    bboxes.append([float(item["x0"]), float(item["y0"]), float(item["x1"]), float(item["y1"])])
        else:
            parse_err = "no JSON array"
    except Exception as e:
        parse_err = str(e)

    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    draw = ImageDraw.Draw(img)
    W, H = img.size
    for bi, b in enumerate(bboxes):
        x0, y0, x1, y1 = b[0]*W, b[1]*H, b[2]*W, b[3]*H
        draw.rectangle([x0, y0, x1, y1], outline="red", width=4)
        draw.text((x0+5, y0+5), f"{{bi}}", fill="red")
    img.save(f"{{OUT_DIR}}/vlm_page_{{printed:03d}}_boxes.png")

    with open(f"{{OUT_DIR}}/vlm_page_{{printed:03d}}.txt", "w", encoding="utf-8") as f:
        f.write(f"=== Page {{printed}} model={{MODEL}} ===\\n")
        f.write(f"Image: {{W}}x{{H}}  bboxes: {{len(bboxes)}}\\n")
        for bi, b in enumerate(bboxes):
            f.write(f"  {{bi}}: x0={{b[0]:.3f}} y0={{b[1]:.3f}} x1={{b[2]:.3f}} y1={{b[3]:.3f}}\\n")
        if parse_err:
            f.write(f"PARSE ERROR: {{parse_err}}\\n")
        f.write("\\n--- Raw VLM response ---\\n")
        f.write(response_text)

    results[printed] = {{"bboxes": bboxes, "parse_err": parse_err, "raw_len": len(response_text)}}

doc.close()
print(json.dumps(results, ensure_ascii=False))
"""
    out = _run_in_container(py)
    data = json.loads(out)
    _copy_container_outputs()
    print(f"=== VLM bbox probe ({model}) ===")
    for printed_str, info in data.items():
        printed = int(printed_str)
        if "error" in info:
            print(f"  page {printed}: ERROR {info['error']}")
            continue
        n = len(info["bboxes"])
        perr = f" parse_err={info['parse_err']}" if info["parse_err"] else ""
        print(f"  page {printed}: {n} bboxes raw_len={info['raw_len']}{perr}")
    print(f"\nImages: {HOST_OUT}/vlm_page_*.png")


# ── Subcommand: detect ─────────────────────────────────────────────

def cmd_detect(args):
    pages_filter = None
    if args.pages:
        pages_filter = [int(p) for p in args.pages.split(",")]
    dump = pages_filter is not None
    py = f"""
import sys, json, os, io
sys.path.insert(0, "/workspace")
import fitz, yaml
from PIL import Image, ImageDraw

from dlt.lib.table_regions import detect_table_regions
from dlt.lib.duckdb_reader import get_reader

PDF = {PDF_PATH!r}
CONFIG_PATH = {CONFIG_PATH!r}
TARGET = {pages_filter!r}
DUMP = {dump!r}
OUT_DIR = {CONTAINER_OUT!r}
os.makedirs(OUT_DIR, exist_ok=True)

with open(CONFIG_PATH) as f:
    cfg_full = yaml.safe_load(f)
cfg = cfg_full.get("table_detection", {{}})

c = get_reader(["bronze_tabletop"])
rows = c.execute(
    "SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts ORDER BY page_index"
).fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}
idx_to_printed = {{r[1]: r[0] for r in rows}}

toc_rows = c.execute(
    "SELECT page_start, title FROM bronze_tabletop.toc_raw "
    "WHERE is_table = TRUE AND is_excluded = FALSE"
).fetchall()
toc_counts = {{}}
toc_titles = {{}}
for p, t in toc_rows:
    toc_counts[p] = toc_counts.get(p, 0) + 1
    toc_titles.setdefault(p, []).append(t)

# Pages covered by any excluded ToC section — caller responsibility to skip.
# Reads excluded titles from the per-book yaml so probe matches what bronze
# WILL exclude on next run, not just what's currently in the catalog.
excluded_titles = set(cfg_full.get("exclude_chapters", []))
all_toc = c.execute(
    "SELECT page_start, page_end, title FROM bronze_tabletop.toc_raw"
).fetchall()
excluded_pages = set()
for ps, pe, title in all_toc:
    is_excluded = title in excluded_titles
    if not is_excluded:
        # Also honor any catalog-level is_excluded already set
        try:
            row = c.execute(
                "SELECT is_excluded FROM bronze_tabletop.toc_raw "
                "WHERE title = ? AND page_start = ?", [title, ps]
            ).fetchone()
            is_excluded = bool(row[0]) if row else False
        except Exception:
            pass
    if is_excluded:
        for p in range(ps, min(pe, 9999) + 1):
            excluded_pages.add(p)

doc = fitz.open(PDF)
target_indices = ([printed_to_idx[p] for p in TARGET if p in printed_to_idx]
                  if TARGET else list(range(len(doc))))

per_page = {{}}
for page_idx in target_indices:
    page = doc[page_idx]
    printed = idx_to_printed.get(page_idx, page_idx)
    if printed in excluded_pages:
        continue
    try:
        regions = detect_table_regions(page, cfg)
    except Exception as e:
        per_page[printed] = {{"error": str(e)}}
        continue
    per_page[printed] = {{
        "page_idx": page_idx,
        "regions": [{{
            "bbox": list(r.bbox), "header_bbox": list(r.header_bbox),
            "columns": r.columns, "row_count": r.row_count,
            "col_count": r.col_count, "header_rows": r.header_row_count,
        }} for r in regions],
    }}
    if DUMP and regions:
        pix = page.get_pixmap(dpi=150)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        draw = ImageDraw.Draw(img)
        scale = 150 / 72
        for ri, r in enumerate(regions):
            x0, y0, x1, y1 = [v * scale for v in r.bbox]
            draw.rectangle([x0, y0, x1, y1], outline="red", width=4)
            draw.text((x0 + 5, y0 + 5), f"R{{ri}}", fill="red")
            hx0, hy0, hx1, hy1 = [v * scale for v in r.header_bbox]
            draw.rectangle([hx0, hy0, hx1, hy1], outline="blue", width=2)
        img.save(f"{{OUT_DIR}}/regions_page_{{printed:03d}}.png")

doc.close()

# Summary
all_pages = sorted(set(per_page.keys()) | set(toc_counts.keys()))
matched = under = missing = extra = 0
summary = {{"per_page": {{}}, "toc_counts": toc_counts, "toc_titles": toc_titles}}
for pp in all_pages:
    info = per_page.get(pp, {{"regions": []}})
    if "error" in info:
        summary["per_page"][pp] = {{"error": info["error"], "toc_count": toc_counts.get(pp, 0)}}
        continue
    det = len(info["regions"])
    toc = toc_counts.get(pp, 0)
    if det == 0 and toc > 0:
        missing += 1; status = "MISSING"
    elif det >= toc and toc > 0:
        matched += 1; status = "OK"
    elif det > 0 and toc == 0:
        extra += 1; status = "EXTRA"
    elif det < toc:
        under += 1; status = "UNDER"
    else:
        status = "EMPTY"
    summary["per_page"][pp] = {{
        "detected": det, "toc_count": toc, "status": status, "regions": info["regions"]
    }}

summary["totals"] = {{
    "pages_scanned": len(target_indices),
    "pages_with_regions": sum(1 for v in summary["per_page"].values() if isinstance(v, dict) and v.get("detected", 0) > 0),
    "matched_pages": matched, "missing_pages": missing,
    "under_pages": under, "extra_pages": extra,
    "total_regions": sum(v.get("detected", 0) for v in summary["per_page"].values() if isinstance(v, dict)),
    "total_toc_is_table": sum(toc_counts.values()),
}}
print(json.dumps(summary, ensure_ascii=False))
"""
    out = _run_in_container(py)
    data = json.loads(out)
    HOST_OUT.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    if dump:
        _copy_container_outputs()

    t = data["totals"]
    print("=== Font-switch table region detection ===")
    print(f"  Pages scanned:       {t['pages_scanned']}")
    print(f"  Pages with regions:  {t['pages_with_regions']}")
    print(f"  Matched (det>=toc):  {t['matched_pages']}")
    print(f"  Missing (det=0,toc>0): {t['missing_pages']}")
    print(f"  Under (0<det<toc):   {t['under_pages']}")
    print(f"  Extra (det>0,toc=0): {t['extra_pages']}")
    print(f"  Total regions:       {t['total_regions']}")
    print(f"  Total ToC is_table:  {t['total_toc_is_table']}")
    print(f"\nFull data: {SUMMARY_JSON}")


# ── Subcommand: inspect ────────────────────────────────────────────

def cmd_inspect(args):
    if not SUMMARY_JSON.exists():
        print(f"Not found: {SUMMARY_JSON}. Run 'detect' first.")
        sys.exit(1)
    data = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    per_page = data["per_page"]

    if not args.targets:
        print("=== Totals ===")
        for k, v in data["totals"].items():
            print(f"  {k}: {v}")
        by_status = {}
        for v in per_page.values():
            if isinstance(v, dict):
                s = v.get("status", "?")
                by_status[s] = by_status.get(s, 0) + 1
        print("\n=== By status ===")
        for s, n in sorted(by_status.items()):
            print(f"  {s}: {n}")
        return

    statuses = set(t for t in args.targets if not t.isdigit())
    page_nums = set(int(t) for t in args.targets if t.isdigit())

    if statuses:
        pages = [(int(k), v) for k, v in per_page.items()
                 if isinstance(v, dict) and v.get("status") in statuses]
        for pp, info in sorted(pages):
            det = info.get("detected", 0)
            toc = info.get("toc_count", 0)
            titles = data["toc_titles"].get(str(pp), [])
            hint = "; ".join(t[:35] for t in titles[:2])
            print(f"  page {pp:4} [{info['status']:7}] det={det} toc={toc}  {hint}")

    if page_nums:
        for pp in sorted(page_nums):
            info = per_page.get(str(pp))
            if not info:
                print(f"page {pp}: not found")
                continue
            det = info.get("detected", 0)
            toc = info.get("toc_count", 0)
            titles = data["toc_titles"].get(str(pp), [])
            print(f"=== page {pp} [{info.get('status', '?')}] det={det} toc={toc} ===")
            for t in titles:
                print(f"  toc: {t}")
            for ri, r in enumerate(info.get("regions", [])):
                print(f"  R{ri}: bbox={r['bbox']} rows={r['row_count']} cols={r['col_count']} hdr_rows={r['header_rows']}")
                print(f"       columns={r['columns']}")


# ── Subcommand: debug ──────────────────────────────────────────────

def cmd_debug(args):
    printed = args.page
    py = f"""
import sys
sys.path.insert(0, "/workspace")
import fitz, yaml

from dlt.lib.table_regions import (
    _flatten_spans, _is_header_span, _group_into_rows,
    _cluster_header_rows, _column_ranges_from_cluster, _row_x_range,
    detect_table_regions,
)
from dlt.lib.duckdb_reader import get_reader

with open({CONFIG_PATH!r}) as f:
    cfg = yaml.safe_load(f).get("table_detection", {{}})

c = get_reader(["bronze_tabletop"])
rows = c.execute("SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts").fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}

PRINTED = {printed}
page_idx = printed_to_idx[PRINTED]
doc = fitz.open({PDF_PATH!r})
page = doc[page_idx]
print(f"=== Page {{PRINTED}} (idx {{page_idx}}) ===")
print(f"cfg: {{cfg}}")
print()

spans = _flatten_spans(page.get_text("dict"))
print(f"Total spans: {{len(spans)}}")

header_spans = [s for s in spans if _is_header_span(s, cfg)]
print(f"Header spans matching cfg: {{len(header_spans)}}")
for s in header_spans:
    print(f"  bbox={{s['bbox']}}  {{s['text']!r}}")
print()

y_tol = cfg.get("row_y_tolerance", 3.0)
header_rows = _group_into_rows(header_spans, y_tol)
print(f"Header rows: {{len(header_rows)}}")
for ri, row in enumerate(header_rows):
    print(f"  row {{ri}}: y={{min(s['bbox'][1] for s in row):.1f}}  spans={{len(row)}}")
    for s in row:
        print(f"    x0={{s['bbox'][0]:.1f}}  {{s['text']!r}}")
print()

clusters = _cluster_header_rows(header_rows, cfg)
print(f"Header clusters: {{len(clusters)}}")
x_tol = cfg.get("column_x_tolerance", 5.0)
for ci, cluster in enumerate(clusters):
    print(f"  cluster {{ci}}: {{len(cluster)}} row(s)")
    cols = _column_ranges_from_cluster(cluster, x_tol)
    print(f"    column ranges: {{cols}}  (count={{len(cols)}})")
    cluster_x = (min(s['bbox'][0] for r in cluster for s in r),
                 max(s['bbox'][2] for r in cluster for s in r))
    print(f"    header x_range: {{cluster_x}}")

print()
print("=== detect_table_regions output ===")
regions = detect_table_regions(page, cfg)
print(f"Detected: {{len(regions)}}")
for ri, r in enumerate(regions):
    print(f"  R{{ri}}: bbox={{r.bbox}}  rows={{r.row_count}}  cols={{r.col_count}}  hdr_rows={{r.header_row_count}}")
    print(f"       columns={{r.columns}}")

all_rows = _group_into_rows(spans, y_tol)
# Print all rows in y range around each header cluster
for ci, cluster in enumerate(clusters):
    cy_top = min(s['bbox'][1] for r in cluster for s in r)
    cy_bot = max(s['bbox'][3] for r in cluster for s in r)
    cols = _column_ranges_from_cluster(cluster, x_tol)
    cluster_x = (min(s['bbox'][0] for r in cluster for s in r),
                 max(s['bbox'][2] for r in cluster for s in r))
    print()
    print(f"=== Rows around cluster {{ci}} (y={{cy_top:.1f}}-{{cy_bot:.1f}}, x_range={{cluster_x}}) ===")
    for ri, row in enumerate(all_rows):
        ry = min(s['bbox'][1] for s in row)
        if cy_top - 5 <= ry <= cy_bot + 80:
            x_range = _row_x_range(row)
            in_band = "   " if (x_range[1] < cluster_x[0] or x_range[0] > cluster_x[1]) else ">>>"
            print(f"  {{in_band}} row {{ri}}: y={{ry:.1f}}  x={{x_range}}  spans={{len(row)}}")
            for s in row[:8]:
                print(f"        x0={{s['bbox'][0]:.1f}}  {{s['font'][:20]}}  {{s['text']!r}}")
            if len(row) > 8:
                print(f"        ... ({{len(row) - 8}} more)")
doc.close()
"""
    sys.stdout.write(_run_in_container(py))


# ── Subcommand: span-map ───────────────────────────────────────────

def cmd_span_map(args):
    pages_filter = None
    if args.pages:
        pages_filter = [int(p) for p in args.pages.split(",")]
    py = f"""
import sys, json
sys.path.insert(0, "/workspace")
import fitz
from dlt.lib.table_regions import extract_page_text_with_span_map
from dlt.lib.duckdb_reader import get_reader

PDF = {PDF_PATH!r}
TARGET = {pages_filter!r}

c = get_reader(["bronze_tabletop"])
rows = c.execute("SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts ORDER BY page_index").fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}
idx_to_printed = {{r[1]: r[0] for r in rows}}

doc = fitz.open(PDF)
target_indices = ([printed_to_idx[p] for p in TARGET if p in printed_to_idx]
                  if TARGET else list(range(len(doc))))

n_pages = 0
n_match = 0
mismatches = []
for page_idx in target_indices:
    page = doc[page_idx]
    expected = page.get_text("text")
    actual, span_map = extract_page_text_with_span_map(page)
    n_pages += 1
    if actual == expected:
        n_match += 1
    else:
        mismatches.append({{
            "page_idx": page_idx,
            "printed": idx_to_printed.get(page_idx, page_idx),
            "expected_len": len(expected),
            "actual_len": len(actual),
            "first_diff": _first_diff(expected, actual),
        }})

doc.close()
print(json.dumps({{"n_pages": n_pages, "n_match": n_match,
                   "mismatches": mismatches[:10]}}, ensure_ascii=False))
"""
    # Inject helper into the in-container code
    helper = """
def _first_diff(a, b):
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            ctx_a = a[max(0,i-20):i+20]
            ctx_b = b[max(0,i-20):i+20]
            return {"pos": i, "expected": repr(ctx_a), "actual": repr(ctx_b)}
    return {"pos": min(len(a), len(b)), "expected": repr(a[-30:]), "actual": repr(b[-30:])}
"""
    py = helper + py
    out = _run_in_container(py)
    data = json.loads(out)
    print(f"=== span-map byte-equivalence check ===")
    print(f"  pages checked: {data['n_pages']}")
    print(f"  matches:       {data['n_match']}")
    print(f"  mismatches:    {data['n_pages'] - data['n_match']}")
    for m in data["mismatches"]:
        print(f"  page {m['printed']}: expected={m['expected_len']} actual={m['actual_len']}")
        d = m["first_diff"]
        print(f"    first diff @ pos {d['pos']}")
        print(f"    expected: {d['expected']}")
        print(f"    actual:   {d['actual']}")


# ── Subcommand: mask ───────────────────────────────────────────────

def cmd_mask(args):
    pages = args.pages
    py = f"""
import sys, json
sys.path.insert(0, "/workspace")
import fitz, yaml
from dlt.lib.table_regions import (
    extract_page_text_with_span_map, detect_table_regions, region_char_ranges,
)
from dlt.lib.duckdb_reader import get_reader

PDF = {PDF_PATH!r}
TARGET = {pages!r}
with open({CONFIG_PATH!r}) as f:
    cfg = yaml.safe_load(f).get("table_detection", {{}})

c = get_reader(["bronze_tabletop"])
rows = c.execute("SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts").fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}

doc = fitz.open(PDF)
results = []
for printed in TARGET:
    if printed not in printed_to_idx:
        continue
    page = doc[printed_to_idx[printed]]
    text, span_map = extract_page_text_with_span_map(page)
    regions = detect_table_regions(page, cfg)
    raw_ranges = []
    for r in regions:
        raw_ranges.extend(region_char_ranges(r, span_map))
    raw_ranges.sort()
    # Coalesce overlapping ranges across all regions
    all_ranges = []
    for s, e in raw_ranges:
        if all_ranges and s <= all_ranges[-1][1]:
            all_ranges[-1] = (all_ranges[-1][0], max(all_ranges[-1][1], e))
        else:
            all_ranges.append((s, e))
    # Apply mask: blank out chars in those ranges (preserving newlines)
    masked = list(text)
    for s, e in all_ranges:
        for i in range(s, min(e, len(masked))):
            if masked[i] != "\\n":
                masked[i] = " "
    masked_text = "".join(masked)
    results.append({{
        "printed": printed,
        "regions": len(regions),
        "ranges": all_ranges,
        "orig_len": len(text),
        "masked_chars": sum(e - s for s, e in all_ranges),
        "original": text,
        "masked": masked_text,
    }})
doc.close()
print(json.dumps(results, ensure_ascii=False))
"""
    out = _run_in_container(py)
    data = json.loads(out)
    HOST_OUT.mkdir(parents=True, exist_ok=True)
    for r in data:
        printed = r["printed"]
        out_path = HOST_OUT / f"mask_page_{printed:03d}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"=== Page {printed} ===\n")
            f.write(f"regions: {r['regions']}\n")
            f.write(f"orig_chars: {r['orig_len']}  masked_chars: {r['masked_chars']}\n")
            f.write(f"ranges: {r['ranges']}\n")
            f.write("\n--- ORIGINAL ---\n")
            f.write(r["original"])
            f.write("\n--- MASKED ---\n")
            f.write(r["masked"])
        print(f"  page {printed}: regions={r['regions']} masked={r['masked_chars']}/{r['orig_len']} chars -> {out_path}")


# ── Main ───────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_fonts = sub.add_parser("fonts", help="font histogram for given pages")
    p_fonts.add_argument("pages", nargs="*", type=int, help="printed page numbers")
    p_fonts.set_defaults(func=cmd_fonts)

    p_vlm = sub.add_parser("vlm", help="VLM bbox probe for given pages")
    p_vlm.add_argument("pages", nargs="*", type=int, help="printed page numbers")
    p_vlm.add_argument("--model", default="minicpm-v:latest", help="ollama model name")
    p_vlm.set_defaults(func=cmd_vlm)

    p_det = sub.add_parser("detect", help="run detect_table_regions across the book")
    p_det.add_argument("--pages", help="comma-separated pages (also enables PNG dump)")
    p_det.set_defaults(func=cmd_detect)

    p_ins = sub.add_parser("inspect", help="inspect last detect summary")
    p_ins.add_argument("targets", nargs="*", help="status names or page numbers")
    p_ins.set_defaults(func=cmd_inspect)

    p_dbg = sub.add_parser("debug", help="verbose detector trace for one page")
    p_dbg.add_argument("page", type=int)
    p_dbg.set_defaults(func=cmd_debug)

    p_sm = sub.add_parser("span-map",
        help="verify extract_page_text_with_span_map matches page.get_text()")
    p_sm.add_argument("--pages", help="comma-separated printed page numbers (default: all)")
    p_sm.set_defaults(func=cmd_span_map)

    p_mask = sub.add_parser("mask",
        help="dump masked vs original page text for a few ToC table pages")
    p_mask.add_argument("pages", nargs="+", type=int, help="printed page numbers")
    p_mask.set_defaults(func=cmd_mask)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
