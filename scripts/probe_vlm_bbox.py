"""Probe: ask minicpm-v for table bboxes on known table pages, draw on page image.

Tests whether VLM bbox accuracy is usable for table detection (Option 3/5).

Output: verify_output/table_probe/vlm_page_<NN>_orig.png  (page image)
        verify_output/table_probe/vlm_page_<NN>_boxes.png (page image with VLM boxes drawn)
        verify_output/table_probe/vlm_page_<NN>.txt        (raw VLM response + parsed bboxes)
"""
import subprocess
import sys
from pathlib import Path

CONTAINER = "lakehouse-workspace"
PDF_PATH = "/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf"
HOST_OUT = Path("verify_output/table_probe")
HOST_OUT.mkdir(parents=True, exist_ok=True)
CONTAINER_OUT = "/workspace/verify_output/table_probe"

PAGES = sys.argv[1:] if len(sys.argv) > 1 else ["19", "27", "76", "90", "94", "121"]

py_code = f"""
import sys, json, base64, os
sys.path.insert(0, "/workspace")
import fitz
import requests
from PIL import Image, ImageDraw, ImageFont
import io

PDF = {PDF_PATH!r}
TARGET_PRINTED = {[int(p) for p in PAGES]!r}
OUT_DIR = {CONTAINER_OUT!r}
os.makedirs(OUT_DIR, exist_ok=True)

VLM_URL = "http://host.docker.internal:11434"
VLM_MODEL = "minicpm-v:latest"
DPI = 200

PROMPT = (
    "This is a page from a tabletop RPG rulebook. "
    "Identify every TABLE on this page (rows and columns of data, NOT prose paragraphs). "
    "For each table, return its bounding box as normalized coordinates from 0.0 to 1.0 "
    "where (0,0) is top-left and (1,1) is bottom-right. "
    "Return ONLY a JSON array, no commentary, in this exact format: "
    '[{{"x0": 0.1, "y0": 0.2, "x1": 0.9, "y1": 0.4}}, ...]. '
    "If there are no tables, return []."
)

# Build printed → idx map from bronze
from dlt.lib.duckdb_reader import get_reader
c = get_reader(["bronze_tabletop"])
rows = c.execute(
    "SELECT printed_page_num, page_index FROM bronze_tabletop.page_texts"
).fetchall()
printed_to_idx = {{r[0]: r[1] for r in rows}}

doc = fitz.open(PDF)
results = {{}}

for printed in TARGET_PRINTED:
    if printed not in printed_to_idx:
        results[printed] = {{"error": "page not in bronze"}}
        continue
    page_idx = printed_to_idx[printed]
    page = doc[page_idx]
    pix = page.get_pixmap(dpi=DPI)
    img_bytes = pix.tobytes("png")

    # Save original
    orig_path = f"{{OUT_DIR}}/vlm_page_{{printed:03d}}_orig.png"
    with open(orig_path, "wb") as f:
        f.write(img_bytes)

    img_b64 = base64.b64encode(img_bytes).decode()

    try:
        resp = requests.post(
            f"{{VLM_URL}}/api/generate",
            json={{
                "model": VLM_MODEL,
                "prompt": PROMPT,
                "images": [img_b64],
                "stream": False,
                "options": {{"temperature": 0.0, "num_predict": 1024}},
            }},
            timeout=180,
        )
        resp.raise_for_status()
        response_text = resp.json().get("response", "").strip()
    except Exception as e:
        results[printed] = {{"error": str(e)}}
        continue

    # Parse JSON array
    bboxes = []
    parse_err = None
    try:
        s = response_text.find("[")
        e = response_text.rfind("]") + 1
        if s >= 0 and e > s:
            parsed = json.loads(response_text[s:e])
            for item in parsed:
                if isinstance(item, dict) and all(k in item for k in ("x0", "y0", "x1", "y1")):
                    bboxes.append([float(item["x0"]), float(item["y0"]),
                                   float(item["x1"]), float(item["y1"])])
        else:
            parse_err = "no JSON array found"
    except Exception as e:
        parse_err = str(e)

    # Draw boxes on image
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    draw = ImageDraw.Draw(img)
    W, H = img.size
    for bi, b in enumerate(bboxes):
        x0, y0, x1, y1 = b[0] * W, b[1] * H, b[2] * W, b[3] * H
        draw.rectangle([x0, y0, x1, y1], outline="red", width=4)
        draw.text((x0 + 5, y0 + 5), f"{{bi}}", fill="red")

    boxes_path = f"{{OUT_DIR}}/vlm_page_{{printed:03d}}_boxes.png"
    img.save(boxes_path)

    # Save text result
    text_path = f"{{OUT_DIR}}/vlm_page_{{printed:03d}}.txt"
    with open(text_path, "w", encoding="utf-8") as f:
        f.write(f"=== Page {{printed}} (idx {{page_idx}}) ===\\n")
        f.write(f"Image size: {{W}}x{{H}}\\n")
        f.write(f"Parsed bboxes: {{len(bboxes)}}\\n")
        for bi, b in enumerate(bboxes):
            f.write(f"  {{bi}}: x0={{b[0]:.3f}} y0={{b[1]:.3f}} x1={{b[2]:.3f}} y1={{b[3]:.3f}}\\n")
        if parse_err:
            f.write(f"PARSE ERROR: {{parse_err}}\\n")
        f.write("\\n--- Raw VLM response ---\\n")
        f.write(response_text)

    results[printed] = {{
        "page_idx": page_idx, "img_size": [W, H],
        "bboxes": bboxes, "parse_err": parse_err,
        "raw_len": len(response_text),
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

print("=== VLM bbox probe summary ===")
for printed_str, info in data.items():
    printed = int(printed_str)
    if "error" in info:
        print(f"  page {printed}: ERROR {info['error']}")
        continue
    nb = len(info["bboxes"])
    perr = f" parse_err={info['parse_err']}" if info["parse_err"] else ""
    print(f"  page {printed}: {nb} bboxes  raw_len={info['raw_len']}{perr}")
print()
print(f"Images: verify_output/table_probe/vlm_page_*.png")
print(f"Raw responses: verify_output/table_probe/vlm_page_*.txt")
