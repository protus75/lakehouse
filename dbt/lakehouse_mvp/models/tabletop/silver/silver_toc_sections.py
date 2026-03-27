"""Silver: ToC sections with hierarchy from bronze, sub_headings from entries.

Replaces the SQL model to use stable_keys for deterministic toc_id + parent_toc_id.
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.stable_keys import make_id
    import pandas as pd

    # Get latest run's ToC data
    run_df = session.execute(
        "SELECT max(run_id) as run_id FROM bronze_tabletop.toc_raw"
    ).fetchdf()
    latest_run = run_df.iloc[0]["run_id"]

    toc_df = session.execute(
        f"SELECT source_file, title, page_start, page_end, sort_order, "
        f"coalesce(depth, 0) as depth, coalesce(is_chapter, true) as is_chapter, "
        f"coalesce(is_table, false) as is_table, is_excluded, parent_title "
        f"FROM bronze_tabletop.toc_raw WHERE run_id = '{latest_run}' "
        f"ORDER BY source_file, sort_order"
    ).fetchdf()

    # Get sub_headings from silver_entries
    entries_df = session.execute(
        "SELECT source_file, toc_title, "
        "string_agg(distinct entry_title, '; ' order by entry_title) as sub_headings "
        "FROM silver_tabletop.silver_entries "
        "WHERE entry_title IS NOT NULL "
        "GROUP BY source_file, toc_title"
    ).fetchdf()
    sub_map = {}
    for _, row in entries_df.iterrows():
        sub_map[(row["source_file"], row["toc_title"])] = row["sub_headings"]

    rows = []
    for _, row in toc_df.iterrows():
        sf = row["source_file"]
        title = row["title"]
        parent = row["parent_title"]

        toc_id = make_id("toc_id", {"source_file": sf, "title": title})
        parent_toc_id = make_id("toc_id", {"source_file": sf, "title": parent}) if parent else None

        rows.append({
            "toc_id": toc_id,
            "parent_toc_id": parent_toc_id,
            "source_file": sf,
            "title": title,
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]) if row["page_end"] else None,
            "sort_order": int(row["sort_order"]) if row["sort_order"] else 0,
            "depth": int(row["depth"]),
            "is_chapter": bool(row["is_chapter"]),
            "is_table": bool(row["is_table"]),
            "is_excluded": bool(row["is_excluded"]),
            "parent_title": parent,
            "sub_headings": sub_map.get((sf, title), ""),
            "tables": "",
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["toc_id", "parent_toc_id", "source_file", "title", "page_start",
                 "page_end", "sort_order", "depth", "is_chapter", "is_table",
                 "is_excluded", "parent_title", "sub_headings", "tables"]
    )
