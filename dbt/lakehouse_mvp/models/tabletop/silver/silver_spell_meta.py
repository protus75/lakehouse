"""Silver: structured spell metadata parsed from entry content header.

Parses the key-value metadata block at the start of spell content:
  (School1, School2)
  Reversible
  Sphere: Healing
  Range: Touch
  Components: V, S, M
  Duration: Permanent
  Casting Time: 1 rd.
  Area of Effect: 1 creature
  Saving Throw: None

One row per spell entry. Fields are extracted as-is (no normalization).
"""
import sys
sys.path.insert(0, "/workspace")


def model(dbt, session):
    dbt.config(materialized="table")

    import pandas as pd
    from pathlib import Path
    from dlt.lib.tabletop_cleanup import load_config

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    # Load spell entries (has_metadata = true means metadata block detected)
    entries = session.execute("""
        SELECT entry_id, source_file, entry_title, content, spell_level
        FROM silver_tabletop.silver_entries
        WHERE spell_level IS NOT NULL
    """).fetchdf()

    if entries.empty:
        return pd.DataFrame(columns=[
            "entry_id", "source_file", "school", "sphere", "reversible",
            "range", "components", "duration", "casting_time",
            "area_of_effect", "saving_throw", "material_component_text",
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
        meta = _parse_spell_header(content)
        mat_text = _extract_material_component(content, material_patterns)

        rows.append({
            "entry_id": entry["entry_id"],
            "source_file": entry["source_file"],
            "school": meta.get("school", ""),
            "sphere": meta.get("sphere", ""),
            "reversible": meta.get("reversible", False),
            "range": meta.get("range", ""),
            "components": meta.get("components", ""),
            "duration": meta.get("duration", ""),
            "casting_time": meta.get("casting_time", ""),
            "area_of_effect": meta.get("area_of_effect", ""),
            "saving_throw": meta.get("saving_throw", ""),
            "material_component_text": mat_text,
        })

    return pd.DataFrame(rows)


def _parse_spell_header(content):
    """Parse the metadata block from the start of spell content.

    The header is everything before the first blank line (double newline).
    Lines are either:
      - School line: "(School1, School2)" or "(School1/School2)"
      - Reversible: standalone "Reversible" line
      - Key-value: "Field: value"
    """
    meta = {
        "school": "",
        "sphere": "",
        "reversible": False,
        "range": "",
        "components": "",
        "duration": "",
        "casting_time": "",
        "area_of_effect": "",
        "saving_throw": "",
    }

    # Split on first blank line to get header block.
    # Handle metadata values that wrap past a blank line (e.g. Area of Effect
    # split across lines by OCR). If the text after a blank line starts lowercase
    # or with "in " / "plus " etc., it's a continuation, not the description.
    header_lines = []
    found_break = False
    for line in content.split("\n"):
        if line.strip() == "":
            if found_break:
                break  # second blank line = definitely end of header
            found_break = True
            continue
        if found_break:
            # Check if this is a continuation or real description
            s = line.strip()
            if s[:1].islower() or s.startswith("in ") or s.startswith("plus "):
                # Continuation of previous field value — append to last line
                if header_lines:
                    header_lines[-1] = header_lines[-1] + " " + s
                found_break = False
                continue
            else:
                break  # new sentence = description starts
        header_lines.append(line)
    header = "\n".join(header_lines)

    # Field name mapping (content label -> dict key)
    field_map = {
        "range": "range",
        "components": "components",
        "duration": "duration",
        "casting time": "casting_time",
        "area of effect": "area_of_effect",
        "saving throw": "saving_throw",
        "sphere": "sphere",
    }

    for line in header.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue

        # School line: starts with ( and ends with )
        if stripped.startswith("(") and stripped.endswith(")"):
            school_text = stripped[1:-1].strip()
            # Remove trailing "Reversible" if present
            if school_text.endswith("Reversible"):
                school_text = school_text[:-len("Reversible")].rstrip(", ")
                meta["reversible"] = True
            # Normalize separators: "/" and "," both become ", "
            schools = [s.strip() for s in school_text.replace("/", ",").split(",") if s.strip()]
            meta["school"] = ", ".join(schools)
            continue

        # Standalone Reversible
        if stripped.lower() == "reversible":
            meta["reversible"] = True
            continue

        # Key-value lines
        if ":" in stripped:
            key, _, value = stripped.partition(":")
            key_lower = key.strip().lower()
            if key_lower in field_map:
                meta[field_map[key_lower]] = value.strip()

    return meta


def _extract_material_component(content, patterns):
    """Extract material component text from the end of spell content.

    Returns the material component paragraph(s) or empty string.
    """
    lines = content.rstrip().split("\n")
    for i in range(len(lines) - 1, max(len(lines) - 8, -1), -1):
        stripped = lines[i].strip()
        for pat in patterns:
            if stripped.lower().startswith(pat.lower()):
                return "\n".join(lines[i:]).strip()
    return ""
