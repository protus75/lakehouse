"""Silver: clean entry descriptions with metadata header and material footer stripped.

For spells: strips the key-value metadata block and material component text,
leaving only the prose description.

For non-spells: content passes through as-is.
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    import pandas as pd
    from pathlib import Path
    from dlt.lib.tabletop_cleanup import load_config

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    entries = session.execute("""
        SELECT entry_id, source_file, entry_title, content, spell_level
        FROM silver_tabletop.silver_entries
        WHERE content IS NOT NULL
    """).fetchdf()

    if entries.empty:
        return pd.DataFrame(columns=[
            "entry_id", "source_file", "content",
        ])

    # Config for material component patterns
    sf = entries["source_file"].iloc[0]
    config = load_config(Path(sf), configs_dir)

    material_patterns = config.get("material_component_patterns", [
        "The material component",
        "The material components",
        "The materials for",
        "The spell requires",
        "The spell's material",
        "Material component:",
        "Material components:",
    ])

    rows = []
    for _, entry in entries.iterrows():
        content = entry["content"] or ""
        is_spell = entry["spell_level"] is not None and pd.notna(entry["spell_level"])

        if is_spell:
            description = _strip_spell_header(content)
            description = _strip_material_footer(description, material_patterns)
        else:
            description = content

        description = description.strip()
        if description:
            rows.append({
                "entry_id": entry["entry_id"],
                "source_file": entry["source_file"],
                "content": description,
            })

    return pd.DataFrame(rows)


def _strip_spell_header(content):
    """Remove the metadata header block from spell content.

    Handles metadata values that wrap past a blank line (OCR artifact).
    """
    found_break = False
    desc_start = 0
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == "":
            if found_break:
                desc_start = i + 1
                break
            found_break = True
            continue
        if found_break:
            s = line.strip()
            if s[:1].islower() or s.startswith("in ") or s.startswith("plus "):
                # Continuation of metadata value
                found_break = False
                continue
            else:
                desc_start = i
                break
        desc_start = i + 1
    return "\n".join(lines[desc_start:])


def _strip_material_footer(content, patterns):
    """Remove material component text from the end of content."""
    lines = content.rstrip().split("\n")
    for i in range(len(lines) - 1, max(len(lines) - 8, -1), -1):
        stripped = lines[i].strip()
        for pat in patterns:
            if stripped.lower().startswith(pat.lower()):
                return "\n".join(lines[:i]).rstrip()
    return content
