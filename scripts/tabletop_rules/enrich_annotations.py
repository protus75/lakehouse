"""Enrich gold layer with AI combat/popular annotations. Resumable.

Run: docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_annotations.py
"""
import sys
sys.path.insert(0, "/workspace")

import json
import pyarrow as pa
import requests
from datetime import datetime, timezone
from pathlib import Path
from dlt.lib.tabletop_cleanup import load_config, _log
from dlt.lib.duckdb_reader import get_reader
from dlt.lib.iceberg_catalog import write_iceberg

CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def call_ollama_json(prompt: str, url: str, model: str) -> dict | None:
    try:
        resp = requests.post(
            f"{url}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=300,
        )
        if resp.status_code == 200:
            text = resp.json().get("response", "").strip()
            if "{" in text:
                json_str = text[text.index("{"):text.rindex("}") + 1]
                return json.loads(json_str)
    except Exception as e:
        _log(f"  Ollama error: {e}")
    return None


def main():
    conn = get_reader()

    # Get config
    sf_row = conn.execute("SELECT source_file FROM silver_tabletop.silver_files LIMIT 1").fetchone()
    if not sf_row:
        _log("No silver data found")
        conn.close()
        return

    config = load_config(Path(sf_row[0]), CONFIGS_DIR)
    gold_config = config.get("gold", {})
    ollama_url = gold_config.get("ollama_url", "http://host.docker.internal:11434")
    ollama_model = gold_config.get("ollama_model", "llama3:70b")
    prompt_template = gold_config.get("annotation_prompt", "")
    annotate_types = gold_config.get("annotation_entry_types", ["spell", "proficiency"])

    # Get entries that need annotations (not already done)
    placeholders = ",".join([f"'{t}'" for t in annotate_types])
    try:
        entries = conn.execute(f"""
            SELECT e.entry_id, e.source_file, e.entry_title, e.content,
                   i.entry_type
            FROM silver_tabletop.silver_entries e
            JOIN gold_tabletop.gold_entry_index i ON e.entry_id = i.entry_id
            LEFT JOIN gold_tabletop.gold_ai_annotations a ON e.entry_id = a.entry_id
            WHERE a.entry_id IS NULL
            AND i.entry_type IN ({placeholders})
            ORDER BY e.entry_id
        """).fetchall()
    except Exception:
        entries = conn.execute(f"""
            SELECT e.entry_id, e.source_file, e.entry_title, e.content,
                   i.entry_type
            FROM silver_tabletop.silver_entries e
            JOIN gold_tabletop.gold_entry_index i ON e.entry_id = i.entry_id
            WHERE i.entry_type IN ({placeholders})
            ORDER BY e.entry_id
        """).fetchall()

    if not entries:
        _log("All entries already annotated!")
        conn.close()
        return

    _log(f"AI Annotations: {len(entries)} entries to annotate")
    now = datetime.now(timezone.utc)

    for i, (entry_id, source_file, entry_title, content, entry_type) in enumerate(entries):
        entry_type = entry_type or "entry"
        entry_title = entry_title or ""

        if len(content) > 2000:
            content = content[:2000] + "..."

        prompt = prompt_template.format(
            entry_type=entry_type,
            entry_title=entry_title,
            content=content,
        )

        result = call_ollama_json(prompt, ollama_url, ollama_model)

        is_combat = None
        is_popular = None
        if result:
            is_combat = bool(result.get("combat", False))
            is_popular = bool(result.get("popular", False))

        write_iceberg("gold_tabletop", "gold_ai_annotations", pa.table({
            "entry_id": [entry_id], "source_file": [source_file],
            "entry_title": [entry_title], "is_combat": [is_combat],
            "is_popular": [is_popular], "annotated_at": [now],
        }))

        if (i + 1) % 10 == 0:
            _log(f"  {i + 1}/{len(entries)} annotated")

    conn.close()
    _log(f"Done: {len(entries)} entries annotated")


if __name__ == "__main__":
    main()
