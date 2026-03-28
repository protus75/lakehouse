"""Show ToC sections with no entries (toc_coverage failures)."""
import sys
sys.path.insert(0, "/workspace")

from dlt.lib.duckdb_reader import get_reader

c = get_reader(["gold_tabletop"])
rows = c.execute("""
    SELECT toc_id, toc_title, is_chapter, is_table, sort_order, depth
    FROM gold_tabletop.gold_entries
    WHERE entry_id IS NULL AND is_table = false
    ORDER BY sort_order
""").fetchall()

for toc_id, toc_title, is_ch, is_tbl, so, depth in rows:
    tag = "[CH]" if is_ch else "    "
    print(f"{so:>4} {tag} {toc_title}")
