"""Silver: page-position anchors mapping markdown positions to PDF pages.

Reads bronze marker_extractions + page_texts, builds the anchor map
that lets us locate any heading in the markdown on a specific PDF page.
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import _build_page_position_map, load_config
    from pathlib import Path

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    # Get all source files
    files = dbt.source("bronze_tabletop", "files").df()

    all_rows = []
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

        # Strip watermarks from markdown before building anchors
        watermarks_df = session.execute(
            f"SELECT watermark_text FROM bronze_tabletop.watermarks WHERE source_file = '{sf}'"
        ).fetchdf()
        if not watermarks_df.empty:
            wm_set = set(watermarks_df["watermark_text"].tolist())
            lines = [l for l in markdown.split("\n") if l.strip() not in wm_set]
            markdown = "\n".join(lines)

        # Build anchors
        anchors = _build_page_position_map(markdown, page_texts, page_printed, total_pages, config)

        for md_pos, page_idx in anchors:
            printed = page_printed.get(page_idx, page_idx)
            all_rows.append({
                "source_file": sf,
                "markdown_position": md_pos,
                "page_index": page_idx,
                "printed_page_num": printed,
            })

    import pandas as pd
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["source_file", "markdown_position", "page_index", "printed_page_num"]
    )
