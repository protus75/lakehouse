"""Quick test: run build_entries_from_pages and show results."""
import sys
sys.path.insert(0, "/workspace")

from pathlib import Path
from dlt.lib.tabletop_cleanup import (
    load_config, build_entries_from_pages, _detect_watermarks,
)
from dlt.lib.duckdb_reader import get_reader

configs_dir = Path("/workspace/documents/tabletop_rules/configs")
reader = get_reader()

# Get the PHB source file
files_df = reader.execute("SELECT source_file FROM bronze_tabletop.files").fetchdf()
sf = files_df["source_file"].iloc[0]
config = load_config(Path(sf), configs_dir)

# Load page texts
pages_df = reader.execute(
    f"SELECT page_index, page_text, printed_page_num FROM bronze_tabletop.page_texts "
    f"WHERE source_file = '{sf}' ORDER BY page_index"
).fetchdf()
page_texts = dict(zip(
    pages_df["printed_page_num"].astype(int).tolist(),
    pages_df["page_text"].tolist(),
))

# Load ToC
toc_df = reader.execute(
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

# Watermarks
watermarks = _detect_watermarks(pages_df["page_text"].tolist(), len(pages_df))

# Spell list
spell_list = []
try:
    sl_df = reader.execute(
        f"SELECT entry_name, entry_class, entry_level "
        f"FROM bronze_tabletop.spell_list_entries WHERE source_file = '{sf}'"
    ).fetchdf()
    spell_list = sl_df.to_dict("records") if not sl_df.empty else []
except Exception:
    pass

# Authority entries
authority_entries = []
try:
    ae_df = reader.execute(
        f"SELECT entry_name, entry_type, source_table "
        f"FROM bronze_tabletop.authority_table_entries WHERE source_file = '{sf}'"
    ).fetchdf()
    authority_entries = ae_df.to_dict("records") if not ae_df.empty else []
except Exception:
    pass

# Spell metadata
try:
    meta_df = reader.execute(
        f"SELECT entry_name, school, sphere FROM bronze_tabletop.known_entries_raw "
        f"WHERE source_file = '{sf}' AND entry_class IS NOT NULL AND school IS NOT NULL"
    ).fetchdf()
    if not meta_df.empty:
        spell_meta = {}
        for _, r in meta_df.iterrows():
            name = r["entry_name"].lower()
            if name not in spell_meta:
                spell_meta[name] = {"school": r["school"], "sphere": r.get("sphere")}
        for s in spell_list:
            name = (s.get("entry_name") or "").lower()
            if name in spell_meta:
                s["school"] = spell_meta[name].get("school")
                s["sphere"] = spell_meta[name].get("sphere")
except Exception:
    pass

# Build entries
entries = build_entries_from_pages(
    toc_all, page_texts, spell_list, authority_entries, config, watermarks
)

# Output results
out = []
out.append(f"  Watermarks: {len(watermarks)} patterns detected")
out.append(f"  Config: {Path(sf).stem}")
out.append(f"  Entries from pages: {len(entries)}")
out.append(f"Total entries: {len(entries)}")
non_spell = [e for e in entries if not e.get("spell_class")]
spells = [e for e in entries if e.get("spell_class")]
out.append(f"Non-spell: {len(non_spell)}")
out.append(f"Spells: {len(spells)}")
out.append("")

for e in entries:
    toc = e["toc_entry"]["title"]
    title = e.get("entry_title") or e.get("section_title") or "?"
    content = e.get("content", "")
    chars = len(content)
    preview = content[:120].replace("\n", "\n  ")
    out.append(f"[{toc}] {title}  ({chars} chars)")
    out.append(f"  {preview}")
    out.append("")

result = "\n".join(out)
with open("/workspace/cache/entry_matches.txt", "w", encoding="utf-8") as f:
    f.write(result)

# Show problem sections specifically
print("=== KEY SECTIONS CHECK ===")
problem_titles = ["Fighter", "Paladin", "Ranger", "Warrior", "Rogue", "Druid",
                   "Elves", "Halflings", "Humans", "Requirements", "Weapons Allowed",
                   "Granted Powers", "Ethos", "Bard", "Cleric", "Mage", "Thief"]
for e in entries:
    title = e.get("entry_title") or ""
    if title in problem_titles:
        content = e.get("content", "")
        first_line = content.split("\n")[0][:80] if content else ""
        pages = e.get("page_numbers", [])
        toc = e["toc_entry"]["title"]
        print(f"  [{toc}] {title} ({len(content)} chars, pages={pages})")
        print(f"    First line: {first_line}")
        print()

# Show total stats
print(f"\nTotal: {len(entries)} entries, {len(non_spell)} non-spell, {len(spells)} spells")
