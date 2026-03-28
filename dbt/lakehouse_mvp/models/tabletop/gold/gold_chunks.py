"""Gold: query-ready chunks from silver entries.

Splits silver entries into 800-char chunks with 200-char overlap
using the shared library's chunk_entries function.
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import chunk_entries, load_config
    from dlt.lib.stable_keys import make_id
    from pathlib import Path
    from datetime import datetime, timezone
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    entries_df = dbt.ref("silver_entries").df()
    toc_df = dbt.ref("silver_toc_sections").df()

    # Build toc lookup for chunk_entries (needs toc_entry dicts)
    toc_by_title = {}
    for _, row in toc_df.iterrows():
        toc_by_title[(row["source_file"], row["title"])] = {
            "title": row["title"],
            "toc_id": int(row["toc_id"]),
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]),
            "is_excluded": bool(row["is_excluded"]),
        }

    all_chunks = []
    chunk_index_by_entry = {}  # track chunk index per entry for stable keys
    now = datetime.now(timezone.utc)

    # Process per source_file (each book may have different chunking config)
    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), configs_dir)
        sf_entries = entries_df[entries_df["source_file"] == sf]

        # Convert to the dict format chunk_entries expects
        entry_list = []
        for _, row in sf_entries.iterrows():
            toc_key = (row["source_file"], row["toc_title"])
            toc_entry = toc_by_title.get(toc_key, {"title": row["toc_title"]})
            page_str = row["page_numbers"] if row["page_numbers"] else ""
            pages = [int(p) for p in page_str.split(",") if p.strip()]

            entry_list.append({
                "toc_entry": toc_entry,
                "entry_id": int(row["entry_id"]),
                "section_title": row["section_title"],
                "entry_title": row["entry_title"],
                "content": row["content"],
                "page_numbers": pages if pages else [0],
            })

        chunks = chunk_entries(entry_list, config)

        for chunk in chunks:
            toc = chunk["toc_entry"]
            toc_title = toc.get("title", "")
            entry_title = chunk.get("entry_title")

            # Track chunk index per entry for stable key
            entry_key = (sf, toc_title, entry_title)
            idx = chunk_index_by_entry.get(entry_key, 0)
            chunk_index_by_entry[entry_key] = idx + 1

            entry_id = chunk["entry_id"]
            chunk_id = make_id("chunk_id", {
                "source_file": sf, "toc_title": toc_title,
                "entry_title": entry_title, "chunk_index": idx,
            })

            all_chunks.append({
                "chunk_id": chunk_id,
                "entry_id": entry_id,
                "source_file": sf,
                "toc_id": toc.get("toc_id"),
                "section_title": chunk.get("section_title"),
                "entry_title": entry_title,
                "content": chunk["content"],
                "page_numbers": chunk["page_numbers"],
                "char_count": len(chunk["content"]),
                "chunk_type": chunk.get("chunk_type", "content"),
                "chunked_at": now,
            })

    return pd.DataFrame(all_chunks) if all_chunks else pd.DataFrame(
        columns=["chunk_id", "source_file", "toc_id", "section_title",
                 "entry_title", "content", "page_numbers", "char_count",
                 "chunk_type", "chunked_at"]
    )
