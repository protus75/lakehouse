"""Silver: cleaned, chapter-assigned entries from Marker markdown.

Stream-based approach:
1. Build extended ToC (real ToC + spell/authority entries injected in order)
2. Walk markdown stream, match headings to extended ToC in document order
3. Split content between consecutive matched headings
4. Clean entry content (smashed metadata, dedup, orphan merge)
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import (
        load_config, build_extended_toc, build_entries_from_stream,
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
        total_pages = len(page_texts)

        # Load full ToC (all entries: chapters + sub-sections)
        toc_df = session.execute(
            f"SELECT title, page_start, page_end, is_excluded, is_chapter, is_table, parent_title "
            f"FROM bronze_tabletop.toc_raw WHERE source_file = '{sf}' ORDER BY page_start"
        ).fetchdf()

        toc_all = []
        for _, row in toc_df.iterrows():
            entry = {
                "title": row["title"],
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]) if row["page_end"] else 9999,
                "is_excluded": bool(row["is_excluded"]),
                "is_chapter": bool(row["is_chapter"]),
                "is_table": bool(row.get("is_table", False)),
                "parent_title": row.get("parent_title"),
                "sub_headings": [],
                "tables": [],
            }
            toc_all.append(entry)

        # Detect watermarks
        watermarks = _detect_watermarks(page_texts, total_pages)

        # Load spell list entries
        spell_list = []
        try:
            sl_df = session.execute(
                f"SELECT entry_name, entry_class, entry_level "
                f"FROM bronze_tabletop.spell_list_entries WHERE source_file = '{sf}'"
            ).fetchdf()
            spell_list = sl_df.to_dict("records") if not sl_df.empty else []
        except Exception:
            pass

        # Load authority entries
        authority_entries = []
        try:
            ae_df = session.execute(
                f"SELECT entry_name, entry_type, source_table "
                f"FROM bronze_tabletop.authority_table_entries WHERE source_file = '{sf}'"
            ).fetchdf()
            authority_entries = ae_df.to_dict("records") if not ae_df.empty else []
        except Exception:
            pass

        # Load cross-referenced spell metadata (school, sphere)
        spell_meta = {}
        try:
            meta_df = session.execute(
                f"SELECT entry_name, school, sphere FROM bronze_tabletop.known_entries_raw "
                f"WHERE source_file = '{sf}' AND entry_class IS NOT NULL AND school IS NOT NULL"
            ).fetchdf()
            if not meta_df.empty:
                for _, r in meta_df.iterrows():
                    name = r["entry_name"].lower()
                    if name not in spell_meta:
                        spell_meta[name] = {"school": r["school"], "sphere": r.get("sphere")}
        except Exception:
            pass

        # Enrich spell_list with school/sphere from cross-reference
        for s in spell_list:
            name = (s.get("entry_name") or "").lower()
            if name in spell_meta:
                s["school"] = spell_meta[name].get("school")
                s["sphere"] = spell_meta[name].get("sphere")

        # Build extended ToC
        extended_toc = build_extended_toc(toc_all, spell_list, authority_entries, config)

        # Build entries from stream
        entries = build_entries_from_stream(markdown, extended_toc, config, watermarks)

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
