"""Silver: cleaned, chapter-assigned entries from pymupdf page texts.

Page-based approach:
1. ToC gives exact page ranges for every chapter and sub-section
2. pymupdf page_texts gives clean text for every page
3. Slice page texts by ToC ranges, split within pages by section titles
4. Clean entry content (smashed metadata, dedup)
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import (
        load_config, build_entries_from_pages,
        collect_sub_headings, _detect_watermarks,
        strip_leading_title,
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

        # Load page texts (pymupdf) — primary content source
        pages_df = session.execute(
            f"SELECT page_index, page_text, printed_page_num FROM bronze_tabletop.page_texts WHERE source_file = '{sf}' ORDER BY page_index"
        ).fetchdf()
        if pages_df.empty:
            continue
        page_texts = dict(zip(
            pages_df["printed_page_num"].astype(int).tolist(),
            pages_df["page_text"].tolist(),
        ))
        total_pages = len(pages_df)

        # Load full ToC — sort_order preserves book order within same page
        toc_df = session.execute(
            f"SELECT title, page_start, page_end, is_excluded, is_chapter, is_table, parent_title, sort_order "
            f"FROM bronze_tabletop.toc_raw WHERE source_file = '{sf}' ORDER BY sort_order"
        ).fetchdf()

        toc_all = []
        for _, row in toc_df.iterrows():
            toc_all.append({
                "title": row["title"],
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]) if row["page_end"] else 9999,
                "is_excluded": bool(row["is_excluded"]),
                "is_chapter": bool(row["is_chapter"]),
                "is_table": bool(row.get("is_table", False)),
                "parent_title": row.get("parent_title"),
                "sort_order": int(row["sort_order"]) if row.get("sort_order") is not None else 0,
                "sub_headings": [],
                "tables": [],
            })

        # Detect watermarks
        page_text_list = pages_df["page_text"].tolist()
        watermarks = _detect_watermarks(page_text_list, total_pages)

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
        try:
            meta_df = session.execute(
                f"SELECT entry_name, school, sphere, ref_page FROM bronze_tabletop.known_entries_raw "
                f"WHERE source_file = '{sf}' AND entry_class IS NOT NULL AND (school IS NOT NULL OR ref_page IS NOT NULL)"
            ).fetchdf()
            if not meta_df.empty:
                spell_meta = {}
                for _, r in meta_df.iterrows():
                    name = r["entry_name"].lower()
                    ref_page = int(r["ref_page"]) if r.get("ref_page") else None
                    if name not in spell_meta:
                        spell_meta[name] = {"school": r["school"], "sphere": r.get("sphere"),
                                            "ref_page": ref_page}
                    elif ref_page and not spell_meta[name].get("ref_page"):
                        spell_meta[name]["ref_page"] = ref_page
                for s in spell_list:
                    name = (s.get("entry_name") or "").lower()
                    if name in spell_meta:
                        s["school"] = spell_meta[name].get("school")
                        s["sphere"] = spell_meta[name].get("sphere")
                        s["ref_page"] = spell_meta[name].get("ref_page")
        except Exception:
            pass

        # Build entries from page texts
        entries = build_entries_from_pages(
            toc_all, page_texts, spell_list, authority_entries, config, watermarks
        )

        # Collect sub-headings
        collect_sub_headings(entries, toc_all, config)

        # Convert to rows
        for entry in entries:
            toc_entry = entry["toc_entry"]
            content = entry["content"]
            entry_title = entry.get("entry_title")
            if entry_title:
                content = strip_leading_title(content, entry_title)

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
                "entry_title": entry_title,
            }
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
