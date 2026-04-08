"""Probe: per-page font/size histogram + non-body span bboxes for known table pages.

Tests whether tables in the PHB use a distinct (font, size) from body text.
If so, font-switch detection (Option 1/5) is viable.

Output: verify_output/table_probe/fonts_page_<NN>.txt with histogram + spans.
"""
import subprocess
import sys
from pathlib import Path

CONTAINER = "lakehouse-workspace"
PDF_PATH = "/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf"

PAGES = sys.argv[1:] if len(sys.argv) > 1 else ["19", "27", "76", "90", "94", "121"]

out_dir = Path("verify_output/table_probe")
out_dir.mkdir(parents=True, exist_ok=True)

py_code = f"""
import sys, json
sys.path.insert(0, "/workspace")
import fitz
from collections import Counter

PDF = {PDF_PATH!r}
TARGET_PRINTED = {[int(p) for p in PAGES]!r}

doc = fitz.open(PDF)

# Build printed→idx map by scanning page labels
printed_to_idx = {{}}
for i in range(len(doc)):
    label = doc[i].get_label()
    try:
        printed_to_idx[int(label)] = i
    except (ValueError, TypeError):
        pass

# Fallback: assume printed = idx + offset by trying common offsets
if not printed_to_idx:
    # Use bronze page_texts table
    from dlt.lib.duckdb_reader import get_reader
    c = get_reader(["bronze_tabletop"])
    rows = c.execute(
        "SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts"
    ).fetchall()
    printed_to_idx = {{r[0]: r[1] for r in rows}}

results = {{}}
for printed in TARGET_PRINTED:
    if printed not in printed_to_idx:
        results[printed] = {{"error": "page not found"}}
        continue
    page = doc[printed_to_idx[printed]]
    d = page.get_text("dict")

    # Histogram over (font, size_rounded, bold)
    hist = Counter()
    spans = []
    for block in d.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                font = span.get("font", "")
                size = round(span.get("size", 0), 1)
                flags = span.get("flags", 0)
                bold = bool(flags & 16)
                key = (font, size, bold)
                hist[key] += len(span.get("text", ""))
                spans.append({{
                    "font": font, "size": size, "bold": bold,
                    "bbox": [round(x, 1) for x in span.get("bbox", [])],
                    "text": span.get("text", "")[:60],
                }})

    # Body style = mode (most chars)
    body_style = hist.most_common(1)[0][0] if hist else None
    body_chars = hist[body_style] if body_style else 0
    total_chars = sum(hist.values())

    # Non-body spans
    non_body = [s for s in spans
                if (s["font"], s["size"], s["bold"]) != body_style]

    results[printed] = {{
        "page_idx": printed_to_idx[printed],
        "page_size": [round(page.rect.width, 1), round(page.rect.height, 1)],
        "body_style": list(body_style) if body_style else None,
        "body_chars": body_chars,
        "total_chars": total_chars,
        "body_pct": round(100 * body_chars / total_chars, 1) if total_chars else 0,
        "histogram": [
            {{"font": k[0], "size": k[1], "bold": k[2], "chars": v,
              "pct": round(100 * v / total_chars, 1) if total_chars else 0}}
            for k, v in hist.most_common(10)
        ],
        "non_body_count": len(non_body),
        "non_body_spans": non_body[:80],
    }}

doc.close()
print(json.dumps(results, indent=2, ensure_ascii=False))
"""

result = subprocess.run(
    ["docker", "exec", CONTAINER, "python", "-c", py_code],
    capture_output=True, text=True, encoding="utf-8",
)
if result.returncode != 0:
    sys.stderr.write(result.stderr)
    sys.exit(result.returncode)

import json
data = json.loads(result.stdout)

# Write per-page summary files + master summary
master_lines = ["=== PyMuPDF font-switch probe ===", ""]
for printed_str, info in data.items():
    printed = int(printed_str)
    if "error" in info:
        master_lines.append(f"PAGE {printed}: {info['error']}")
        continue

    lines = [f"=== Page {printed} (idx {info['page_idx']}, size {info['page_size']}) ==="]
    lines.append(f"Body style: {info['body_style']} ({info['body_pct']}% of chars)")
    lines.append(f"Total chars: {info['total_chars']}")
    lines.append("")
    lines.append("Histogram (top 10):")
    for h in info["histogram"]:
        marker = "  BODY" if h["pct"] == info["body_pct"] else ""
        lines.append(f"  {h['chars']:6d} ({h['pct']:5.1f}%)  {h['font']:30s} {h['size']:5.1f}  bold={h['bold']!s:5s}{marker}")
    lines.append("")
    lines.append(f"Non-body spans: {info['non_body_count']}")
    lines.append("Sample non-body spans:")
    for s in info["non_body_spans"][:30]:
        lines.append(f"  bbox={s['bbox']}  {s['font'][:20]:20s} {s['size']:4.1f}  bold={s['bold']!s:5s}  {s['text']!r}")
    if info["non_body_count"] > 30:
        lines.append(f"  ... ({info['non_body_count'] - 30} more)")

    out_file = out_dir / f"fonts_page_{printed:03d}.txt"
    out_file.write_text("\n".join(lines), encoding="utf-8")

    # Master summary line
    master_lines.append(f"page {printed}: body={info['body_pct']}%  non_body_spans={info['non_body_count']}  body_style={info['body_style']}")

master_lines.append("")
master_lines.append(f"Per-page detail: verify_output/table_probe/fonts_page_*.txt")
master = "\n".join(master_lines)
print(master)
(out_dir / "fonts_summary.txt").write_text(master, encoding="utf-8")
