"""Silver: cleaned, chapter-assigned entries from Marker markdown.

Reads bronze data + silver_page_anchors, runs the full entry-building pipeline:
1. Build heading-chapter map from page anchors
2. Build entries from Marker headings
3. Clean entry content (smashed metadata, dedup, orphan merge)
4. Collect sub-headings per ToC section

This is the core transform — all cleanup logic from the shared library.
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import (
        load_config, build_heading_chapter_map, build_entries,
        collect_sub_headings, _detect_watermarks,
    )
    from dlt.lib.stable_keys import make_id
    from pathlib import Path
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    files = dbt.source("bronze_tabletop", "files").df()

    all_entries = []

    for _, file_row in files.iterrows():
        sf = file_row["source_file"]
        config = load_config(Path(sf), configs_dir)

        # Load bronze data
        marker_df = session.execute(
            f"SELECT markdown_text FROM bronze_tabletop.marker_extractions WHERE source_file = '{sf}'"
        ).fetchdf()
        if marker_df.empty:
            continue
        markdown = marker_df.iloc[0]["markdown_text"]

        pages_df = session.execute(
            f"SELECT page_index, page_text, printed_page_num FROM bronze_tabletop.page_texts WHERE source_file = '{sf}' ORDER BY page_index"
        ).fetchdf()
        page_texts = pages_df["page_text"].tolist()
        page_printed = dict(zip(pages_df["page_index"].tolist(), pages_df["printed_page_num"].tolist()))
        total_pages = len(page_texts)

        # Load full ToC (all entries: chapters + sub-sections)
        # Check if new schema columns exist (is_chapter, parent_title)
        col_df = session.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = 'bronze_tabletop' AND table_name = 'toc_raw'"
        ).fetchdf()
        cols = set(col_df["column_name"].tolist())
        has_new_schema = "is_chapter" in cols

        if has_new_schema:
            toc_df = session.execute(
                f"SELECT title, page_start, page_end, is_excluded, is_chapter, is_table, parent_title "
                f"FROM bronze_tabletop.toc_raw WHERE source_file = '{sf}' ORDER BY page_start"
            ).fetchdf()
        else:
            toc_df = session.execute(
                f"SELECT title, page_start, page_end, is_excluded "
                f"FROM bronze_tabletop.toc_raw WHERE source_file = '{sf}' ORDER BY page_start"
            ).fetchdf()

        toc_all = []
        toc_sections = []  # chapter-level only (for heading-chapter map page ranges)
        for _, row in toc_df.iterrows():
            is_ch = bool(row["is_chapter"]) if has_new_schema else True
            entry = {
                "title": row["title"],
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]) if row["page_end"] else 9999,
                "is_excluded": bool(row["is_excluded"]),
                "is_chapter": is_ch,
                "is_table": bool(row.get("is_table", False)) if has_new_schema else False,
                "parent_title": row.get("parent_title") if has_new_schema else None,
                "sub_headings": [],
                "tables": [],
            }
            toc_all.append(entry)
            if entry["is_chapter"]:
                toc_sections.append(entry)

        # Load known entries — only entries with a class (actual spells from spell index)
        # General index entries without class are excluded to prevent false matches
        ke_df = session.execute(
            f"SELECT entry_name FROM bronze_tabletop.known_entries_raw WHERE source_file = '{sf}' AND entry_class IS NOT NULL"
        ).fetchdf()
        known_entries = set(ke_df["entry_name"].tolist()) if not ke_df.empty else set()

        # Strip watermarks
        watermarks = _detect_watermarks(page_texts, total_pages)
        if watermarks:
            lines = [l for l in markdown.split("\n") if l.strip() not in watermarks]
            markdown = "\n".join(lines)

        # Build heading-chapter map from page anchors
        heading_chapter_map = build_heading_chapter_map(
            markdown, toc_sections, page_texts, page_printed, total_pages, config
        )

        # Build entries
        entries = build_entries(markdown, heading_chapter_map, known_entries, config, toc_all)

        # Collect sub-headings
        collect_sub_headings(entries, toc_all, config)

        # Convert to rows
        for entry in entries:
            toc_entry = entry["toc_entry"]
            content = entry["content"]

            # Detect metadata presence
            field_names = config.get("metadata_field_names", [])
            has_metadata = any(
                f.lower() + ":" in content.lower() for f in field_names
            ) if field_names else False

            # Detect description presence
            has_description = True
            if has_metadata:
                last_meta_pos = -1
                for f in field_names:
                    idx = content.lower().rfind(f.lower() + ":")
                    if idx > last_meta_pos:
                        last_meta_pos = idx
                if last_meta_pos >= 0:
                    after = content[last_meta_pos:].split("\n", 1)
                    min_desc = config.get("validation", {}).get("min_description_chars", 20)
                    has_description = len(after) >= 2 and len(after[1].strip()) >= min_desc

            row_data = {
                "source_file": sf,
                "toc_title": toc_entry["title"],
                "section_title": entry.get("section_title"),
                "entry_title": entry.get("entry_title"),
            }
            # Include content prefix in hash to disambiguate entries with same/NULL titles
            id_data = {**row_data, "content_prefix": content[:80]}
            all_entries.append({
                "entry_id": make_id("entry_id", id_data),
                "toc_id": make_id("toc_id", {"source_file": sf, "title": toc_entry["title"], "parent_title": toc_entry.get("parent_title") or "", "page_start": str(toc_entry.get("page_start", ""))}),
                **row_data,
                "content": content,
                "school": entry.get("school"),
                "sphere": entry.get("sphere"),
                "spell_class": entry.get("spell_class"),
                "spell_level": entry.get("spell_level"),
                "page_numbers": ",".join(str(p) for p in entry.get("page_numbers", [])),
                "char_count": len(content),
                "has_metadata": has_metadata,
                "has_description": has_description,
            })

    return pd.DataFrame(all_entries) if all_entries else pd.DataFrame(
        columns=["entry_id", "source_file", "toc_title", "section_title",
                 "entry_title", "content", "page_numbers", "char_count",
                 "has_metadata", "has_description"]
    )
