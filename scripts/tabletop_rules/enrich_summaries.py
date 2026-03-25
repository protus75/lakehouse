"""Enrich gold layer with AI summaries. Resumable — skips already-summarized entries.

Run: docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_summaries.py
"""
import sys
sys.path.insert(0, "/workspace")

import duckdb
import requests
from datetime import datetime, timezone
from pathlib import Path
from dlt.lib.tabletop_cleanup import load_config, _log

DB_PATH = "/workspace/db/lakehouse.duckdb"
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def call_ollama(prompt: str, url: str, model: str) -> str | None:
    try:
        resp = requests.post(
            f"{url}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=300,
        )
        if resp.status_code == 200:
            return resp.json().get("response", "").strip()
    except Exception as e:
        _log(f"  Ollama error: {e}")
    return None


def ensure_table(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold_tabletop")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_tabletop.gold_ai_summaries (
            entry_id        INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            entry_title     VARCHAR,
            entry_type      VARCHAR,
            summary         VARCHAR NOT NULL,
            summarized_at   TIMESTAMP NOT NULL
        )
    """)


def main():
    conn = duckdb.connect(DB_PATH)
    ensure_table(conn)

    # Get entries that need summaries (not already done)
    entries = conn.execute("""
        SELECT e.entry_id, e.source_file, e.entry_title, e.content, e.char_count,
               i.entry_type
        FROM silver_tabletop.silver_entries e
        JOIN gold_tabletop.gold_entry_index i ON e.entry_id = i.entry_id
        LEFT JOIN gold_tabletop.gold_ai_summaries s ON e.entry_id = s.entry_id
        WHERE s.entry_id IS NULL
        ORDER BY e.entry_id
    """).fetchall()

    if not entries:
        _log("All entries already summarized!")
        conn.close()
        return

    # Get config from first source file
    sf = entries[0][1]
    config = load_config(Path(sf), CONFIGS_DIR)
    gold_config = config.get("gold", {})
    ollama_url = gold_config.get("ollama_url", "http://host.docker.internal:11434")
    ollama_model = gold_config.get("ollama_model", "llama3:70b")
    min_chars = gold_config.get("min_summary_chars", 200)
    prompt_template = gold_config.get("summary_prompt", "Summarize: {content}")

    # Filter by min chars
    to_process = [e for e in entries if e[4] >= min_chars]
    _log(f"AI Summaries: {len(to_process)} entries to summarize ({len(entries) - len(to_process)} skipped, under {min_chars} chars)")

    now = datetime.now(timezone.utc)

    for i, (entry_id, source_file, entry_title, content, char_count, entry_type) in enumerate(to_process):
        entry_type = entry_type or "entry"
        entry_title = entry_title or ""

        if len(content) > 3000:
            content = content[:3000] + "..."

        prompt = prompt_template.format(
            entry_type=entry_type,
            entry_title=entry_title,
            content=content,
        )

        summary = call_ollama(prompt, ollama_url, ollama_model)

        if summary:
            conn.execute(
                "INSERT OR REPLACE INTO gold_tabletop.gold_ai_summaries VALUES (?, ?, ?, ?, ?, ?)",
                [entry_id, source_file, entry_title, entry_type, summary, now],
            )

            if (i + 1) % 10 == 0:
                _log(f"  {i + 1}/{len(to_process)} summarized")
        else:
            _log(f"  FAILED: {entry_title} (entry_id={entry_id})")

    conn.close()
    _log(f"Done: {len(to_process)} entries summarized")


if __name__ == "__main__":
    main()
