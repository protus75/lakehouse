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
    from pathlib import Path
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    files = dbt.source("bronze_tabletop", "files").df()

    all_entries = []
    entry_id = 0

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

        # Load ToC
        toc_df = session.execute(
            f"SELECT title, page_start, page_end, is_excluded FROM bronze_tabletop.toc_raw WHERE source_file = '{sf}' ORDER BY page_start"
        ).fetchdf()
        toc_sections = []
        for _, row in toc_df.iterrows():
            toc_sections.append({
                "title": row["title"],
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]) if row["page_end"] else 9999,
                "is_excluded": bool(row["is_excluded"]),
                "sub_headings": [],
                "tables": [],
            })

        # Load known entries
        ke_df = session.execute(
            f"SELECT entry_name FROM bronze_tabletop.known_entries_raw WHERE source_file = '{sf}'"
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
        entries = build_entries(markdown, heading_chapter_map, known_entries, config)

        # Collect sub-headings
        collect_sub_headings(entries, toc_sections, config)

        # Convert to rows
        for entry in entries:
            entry_id += 1
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

            all_entries.append({
                "entry_id": entry_id,
                "source_file": sf,
                "toc_title": toc_entry["title"],
                "section_title": entry.get("section_title"),
                "entry_title": entry.get("entry_title"),
                "content": content,
                "school": entry.get("school"),
                "sphere": entry.get("sphere"),
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
