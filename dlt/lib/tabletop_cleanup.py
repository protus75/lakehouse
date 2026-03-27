"""
Pure functions for tabletop RPG PDF content processing.

Extracted from load_tabletop_rules_docs.py — no file I/O, no DuckDB, no PDF reading.
Operates on plain data (strings, lists, dicts).
"""

import re
from pathlib import Path

import yaml


# ── Logging ──────────────────────────────────────────────────────

def _log(msg: str) -> None:
    """Print with flush for real-time monitoring in Docker."""
    print(msg, flush=True)


# ── Config ───────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(filepath: Path, configs_dir: Path) -> dict:
    default_path = configs_dir / "_default.yaml"
    book_path = configs_dir / f"{filepath.stem.replace(' ', '_')}.yaml"
    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}
    if book_path.exists():
        with open(book_path) as f:
            book = yaml.safe_load(f) or {}
        config = _deep_merge(config, book)
        _log(f"  Config: {book_path.name}")
    else:
        _log(f"  Config: defaults")
    return config


# ── ToC line parsing ─────────────────────────────────────────────

def _extract_toc_line(line: str) -> tuple[str, int] | None:
    """Extract (title, page_number) from a ToC line using string ops.
    ToC lines look like: 'Chapter 1: Introduction ......... 5'
    Returns None if not a valid ToC line."""
    stripped = line.strip()
    if not stripped or len(stripped) < 5:
        return None

    # Page number must be at the end — find trailing digits
    rstripped = stripped.rstrip()
    i = len(rstripped) - 1
    while i >= 0 and rstripped[i].isdigit():
        i -= 1
    if i >= len(rstripped) - 1:
        return None  # no trailing number
    page_str = rstripped[i + 1:]
    if not page_str or int(page_str) == 0:
        return None

    # Handle multi-page references like "90, 94" — strip trailing ", NNN" patterns
    before = rstripped[:i + 1].rstrip()
    while before.endswith(",") or (before[-1:].isdigit() and ", " in before[-8:]):
        before = before.rstrip(" ,0123456789")

    # Strip dot leaders: sequences of dots, spaces, dots, ellipsis chars
    while before and before[-1] in '.… ':
        before = before.rstrip('.… ')
    title = before.strip()
    if not title:
        return None

    return (title, int(page_str))


# ── Content filtering ────────────────────────────────────────────

def _should_strip_line(stripped: str, config: dict) -> bool:
    """Check if a line should be stripped from entry content based on config patterns.
    Used to remove school/type annotations, tags, etc. that are metadata not content."""
    strip_patterns = config.get("strip_content_patterns", [])
    for pattern in strip_patterns:
        if re.match(pattern, stripped, re.IGNORECASE):
            return True
    return False


# ── Deduplication ────────────────────────────────────────────────

def _deduplicate_marker_blocks(content: str, field_names: list[str], config: dict) -> str:
    """Remove duplicate metadata blocks from Marker page-boundary re-renders.

    Marker sometimes re-renders the same entry at a page boundary, producing
    a near-duplicate block with the same metadata fields. The duplicate often
    starts with a mid-word fragment. This function splits on metadata blocks,
    keeps the longest/most complete version, and appends any unique trailing text.
    """
    if not field_names:
        return content

    ingestion = config.get("ingestion", {})
    max_fragment = ingestion.get("max_fragment_length", 60)
    max_interblock_fragment = ingestion.get("max_interblock_fragment_length", 80)
    min_desc_block = ingestion.get("min_description_block", 15)
    dedup_sig_chars = ingestion.get("dedup_signature_chars", 80)

    # Find all positions where a metadata field starts a line
    meta_starts = []
    lines = content.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        for field in field_names:
            if stripped.lower().startswith(field.lower() + ":"):
                meta_starts.append(i)
                break

    if len(meta_starts) < 2:
        return content

    # Find metadata block boundaries: a block starts at a metadata field
    # and we look for repeated first-field occurrences
    first_field = lines[meta_starts[0]].strip().split(":")[0].strip().lower()
    block_starts = []
    for i in meta_starts:
        stripped = lines[i].strip()
        if stripped.split(":")[0].strip().lower() == first_field:
            block_starts.append(i)

    if len(block_starts) < 2:
        return content

    # Split into: heading (before first block), then each metadata block
    # Strip any mid-word junk between heading and first metadata block
    heading_lines = []
    for line in lines[:block_starts[0]]:
        stripped = line.strip()
        if stripped and stripped[0].islower() and len(stripped) < max_fragment:
            continue  # mid-word fragment or junk preamble
        heading_lines.append(line)
    blocks = []
    for idx, start in enumerate(block_starts):
        end = block_starts[idx + 1] if idx + 1 < len(block_starts) else len(lines)
        block_lines = lines[start:end]

        # Separate metadata lines from description lines
        meta_end = 0
        for j, line in enumerate(block_lines):
            stripped = line.strip()
            if any(stripped.lower().startswith(f.lower() + ":")
                   for f in field_names):
                meta_end = j + 1

        meta = "\n".join(block_lines[:meta_end])
        # Strip preamble junk between blocks (mid-word fragments, duplicate headings)
        desc_lines = block_lines[meta_end:]
        # Remove leading blank lines and lines starting with lowercase (mid-word fragments)
        while desc_lines:
            stripped = desc_lines[0].strip()
            if not stripped:
                desc_lines.pop(0)
            elif stripped[0].islower() and len(stripped) < max_interblock_fragment:
                desc_lines.pop(0)
            else:
                break
        desc = "\n".join(desc_lines).strip()
        blocks.append({"meta": meta, "desc": desc})

    # Keep the block with the most complete metadata
    best = max(blocks, key=lambda b: len(b["meta"]))

    # Collect unique descriptions from all blocks
    all_descs = []
    seen = set()
    for block in blocks:
        if block["desc"] and len(block["desc"]) > min_desc_block:
            sig = block["desc"][:dedup_sig_chars]
            if sig not in seen:
                seen.add(sig)
                all_descs.append(block["desc"])

    # Reassemble: heading + best metadata + all unique descriptions
    result = "\n".join(heading_lines).rstrip()
    if result and best["meta"]:
        result += "\n" + best["meta"]
    elif best["meta"]:
        result = best["meta"]

    for desc in all_descs:
        result += "\n\n" + desc

    return result.strip()


# ── Content cleanup ──────────────────────────────────────────────

def _clean_entry_content(content: str, config: dict) -> str:
    """Clean entry content at ingestion time.

    Fixes Marker artifacts so stored data is clean:
    - Deduplicate metadata blocks from page-boundary re-renders
    - Split smashed metadata fields onto separate lines
    - Strip leading spaces from all lines
    - Clean partial image references
    - Collapse excessive blank lines
    """
    field_names = config.get("metadata_field_names", [])
    field_names_lower = [f.lower() for f in field_names]

    lines = []
    for line in content.split("\n"):
        stripped = line.lstrip()
        # Skip image artifacts
        if stripped.startswith("![") or (stripped and stripped[0].islower() and stripped.endswith((".jpeg", ".png", ".jpeg)", ".png)"))):
            continue

        # Split smashed metadata fields onto separate lines
        # e.g. "Sphere: All Range: 60 yds." -> "Sphere: All\nRange: 60 yds."
        if field_names:
            for fname in field_names:
                # Find field name preceded by space (smashed onto previous field's line)
                idx = stripped.find(" " + fname + ":")
                if idx < 0:
                    idx = stripped.find("  " + fname + ":")
                if idx > 0:
                    lines.append(stripped[:idx].rstrip())
                    stripped = stripped[idx:].lstrip()

        # Split description smashed after metadata value
        # e.g. "Saving Throw: Special  This spell is..." -> two lines
        if field_names and "  " in stripped:
            for fname in field_names:
                if stripped.lower().startswith(fname.lower() + ":"):
                    # Find double-space followed by uppercase letter
                    pos = stripped.find("  ", len(fname) + 1)
                    while pos > 0:
                        after = stripped[pos:].lstrip()
                        if after and after[0].isupper():
                            lines.append(stripped[:pos].rstrip())
                            stripped = after
                            break
                        pos = stripped.find("  ", pos + 2)
                    break

        lines.append(stripped)

    content = "\n".join(lines)
    content = re.sub(r"\n{3,}", "\n\n", content)

    # Apply config-driven substitutions (OCR artifact fixes)
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            content = content.replace(sub[0], sub[1])

    # Strip residual HTML tags from Marker output (tag list from config)
    html_tags = config.get("strip_html_tags", [])
    if html_tags:
        tag_pattern = "|".join(re.escape(t) for t in html_tags)
        content = re.sub(rf"</?(?:{tag_pattern})(?:\s[^>]*)?>", "", content)

    # Deduplicate after all other cleaning
    content = _deduplicate_marker_blocks(content, field_names, config)

    return content.strip()


# ── Metadata / orphan detection ──────────────────────────────────

def _has_metadata_but_no_description(content: str, config: dict) -> bool:
    """Check if entry has metadata fields but little/no description after them."""
    field_names = config.get("metadata_field_names", [])
    if not field_names:
        return False
    last_meta_pos = -1
    for field in field_names:
        idx = content.lower().rfind(field.lower() + ":")
        if idx > last_meta_pos:
            last_meta_pos = idx
    if last_meta_pos < 0:
        return False
    min_desc = config.get("validation", {}).get("min_description_chars", 20)
    after_meta = content[last_meta_pos:].split("\n", 1)
    return len(after_meta) < 2 or len(after_meta[1].strip()) < min_desc


def _is_orphan_continuation(content: str) -> bool:
    """Check if entry content looks like an orphan continuation (split from previous entry).
    Looks past the heading line to check if the first body line starts with lowercase/mid-word."""
    lines = [l for l in content.split("\n") if l.strip()]
    if not lines:
        return False
    first = lines[0].strip()
    if first.startswith("#"):
        for line in lines[1:]:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return stripped[0].islower()
        return False
    return bool(first) and first[0].islower()


# ── Orphan merging ───────────────────────────────────────────────

def _merge_orphan_entries(entries: list[dict], config: dict) -> list[dict]:
    """Merge duplicate and orphan entries caused by Marker page-boundary splits.

    Two passes:
    1. Group-merge: entries with same (toc_title, entry_title) where later fragments
       start with lowercase/mid-word text get folded into the first occurrence.
       Handles Marker duplicating headings across page boundaries.
    2. Hungry+orphan: entries with metadata but no description absorb the next
       orphan continuation entry (different title, starts lowercase).
    """
    if not config or len(entries) < 2:
        return entries

    # Pass 1: merge same-title fragments into first occurrence
    primary = {}  # (toc_title, entry_title) -> index in merged list
    merged = []
    fold_count = 0

    dedup_sig_chars = config.get("ingestion", {}).get("dedup_signature_chars", 80)

    for entry in entries:
        title = entry.get("entry_title")
        toc_title = entry["toc_entry"]["title"]
        key = (toc_title, title) if title else None

        if key and key in primary:
            # Same (toc_title, entry_title) seen before — merge into primary.
            # Strip the duplicate heading, append only unique content.
            orphan_content = entry["content"]
            orphan_content = re.sub(r"^#{1,4}\s+.+\n?", "", orphan_content).strip()
            if orphan_content:
                primary_content = merged[primary[key]]["content"]
                # Skip if content is a near-duplicate of what we already have
                if orphan_content[:dedup_sig_chars] not in primary_content:
                    merged[primary[key]]["content"] += "\n\n" + orphan_content
                fold_count += 1
            continue

        idx = len(merged)
        merged.append(dict(entry))
        if key and key not in primary:
            primary[key] = idx

    # Pass 2: hungry + orphan (different titles, consecutive)
    if len(merged) < 2:
        if fold_count:
            _log(f"  Orphan merges: {fold_count} fragments folded")
        return merged

    result = []
    hungry_count = 0
    i = 0
    while i < len(merged):
        entry = dict(merged[i])

        while i + 1 < len(merged):
            next_entry = merged[i + 1]
            if entry["toc_entry"] != next_entry["toc_entry"]:
                break
            if not _has_metadata_but_no_description(entry["content"], config):
                break
            if not _is_orphan_continuation(next_entry["content"]):
                break
            orphan_content = next_entry["content"]
            orphan_content = re.sub(r"^#{1,4}\s+.+\n?", "", orphan_content).strip()
            if orphan_content:
                entry["content"] = entry["content"] + "\n\n" + orphan_content
            hungry_count += 1
            i += 1

        result.append(entry)
        i += 1

    total = fold_count + hungry_count
    if total:
        _log(f"  Orphan merges: {fold_count} fragments folded, {hungry_count} descriptions recovered")
    return result


# ── Whitelist check ──────────────────────────────────────────────

def _is_whitelist_section(toc_entry: dict | None, config: dict | None) -> bool:
    """Check if a ToC section uses known_entries whitelist for heading filtering.
    Configured via whitelist_sections in config."""
    if not toc_entry or not config:
        return False
    whitelist = config.get("whitelist_sections", [])
    title_lower = toc_entry.get("title", "").lower()
    return any(w.lower() in title_lower for w in whitelist)


# ── Entry building ───────────────────────────────────────────────

def _is_valid_section_heading(heading: str, toc_sections: list[dict], config: dict) -> bool:
    """Check if an H1/H2 heading is a legitimate section heading vs Marker artifact.

    Marker generates garbage H1/H2 headings at page boundaries from running headers
    (e.g. '# The', '# Good.', '# Player'). These must NOT become section boundaries.

    Uses the full ToC (chapters + sub-sections) as ground truth. A heading is valid if:
    1. It matches any ToC entry title (exact match on full title or descriptive part)
    2. It matches a section_parsing key or sub-section pattern from config
    3. It matches a valid_section_headings entry from config (manual overrides)
    """
    clean = heading.strip().rstrip(".")
    if not clean:
        return False
    clean_lower = clean.lower()

    # 1. Check against ALL ToC entries (chapters + sub-sections) — exact match
    for section in toc_sections:
        title = section.get("title", "")
        title_lower = title.lower()
        if clean_lower == title_lower:
            return True
        # Also match on descriptive part after "Chapter N:" prefix
        if ":" in title_lower:
            title_desc = title_lower.split(":", 1)[-1].strip()
            if clean_lower == title_desc:
                return True

    # 2. Check section_parsing keys from config (exact match)
    for sec_key in (config.get("section_parsing", {}) or {}):
        if clean_lower == sec_key.lower():
            return True

    # 3. Check sub-section patterns (e.g. "First-Level Spells")
    for sec_key, sec_cfg in (config.get("section_parsing", {}) or {}).items():
        sub_pat = sec_cfg.get("sub_section_pattern", "")
        if sub_pat and re.match(sub_pat, clean, re.IGNORECASE):
            return True

    # 4. Check valid_section_headings from config (manual overrides for edge cases)
    valid_headings = config.get("valid_section_headings", [])
    if valid_headings:
        valid_set = set(h.lower() for h in valid_headings)
        if clean_lower in valid_set:
            return True

    return False


def build_entries(
    markdown: str,
    heading_chapter_map: dict[int, dict],
    known_entries: set[str],
    config: dict = None,
    toc_sections: list[dict] = None,
) -> list[dict]:
    """Parse Marker markdown into entries using heading-chapter map for ToC assignment.

    No page splitting. Marker's continuous markdown is parsed by headings.
    Each heading's chapter comes from heading_chapter_map (page-position based).
    known_entries whitelist only applies in spell sections -- non-spell sections
    treat every H3/H4 heading as a new entry."""
    # Config lookups
    level_mapping = config.get("spell_level_mapping", {}) if config else {}
    type_mapping = config.get("entry_type_mapping", {}) if config else {}
    _toc_sections = toc_sections or []

    def _toc_spell_class(toc_entry: dict | None) -> str | None:
        """Determine spell class from ToC entry using entry_type_mapping config.
        Returns 'wizard', 'priest', or None."""
        if not toc_entry:
            return None
        title = toc_entry.get("title", "")
        for pattern, entry_type in type_mapping.items():
            if entry_type == "spell" and pattern.lower() in title.lower():
                # The mapping key tells us the class: "Wizard Spells" → wizard
                key_lower = pattern.lower()
                if "wizard" in key_lower:
                    return "wizard"
                if "priest" in key_lower:
                    return "priest"
        return None

    entries = []
    current_toc = None
    current_page = 0
    current_section = None
    current_sub_section = None
    current_spell_class = None
    current_spell_level = None
    current_entry = None
    current_content = []
    current_school = None
    current_sphere = None
    current_printed_page = 0

    def _extract_school_from_raw(raw_content: str) -> str | None:
        """Extract spell school from raw content before cleanup strips it.
        Sources: heading parenthetical, standalone (School) line, School: field."""
        known_schools = {
            "abjuration", "alteration", "conjuration", "conjuration/summoning",
            "divination", "enchantment", "enchantment/charm", "evocation",
            "illusion", "illusion/phantasm", "invocation", "invocation/evocation",
            "necromancy", "universal", "all schools",
        }
        for line in raw_content.split("\n"):
            stripped = line.strip()
            # Heading parenthetical: #### **Fireball** (Evocation)
            if stripped.startswith("#"):
                paren_start = stripped.rfind("(")
                paren_end = stripped.rfind(")")
                if paren_start > 0 and paren_end > paren_start:
                    school = stripped[paren_start + 1:paren_end].replace("*", "").strip()
                    # Accept if any part matches a known school (comma or slash separated)
                    parts = [p.strip().lower() for p in school.replace("/", ",").split(",")]
                    if any(p in known_schools for p in parts):
                        return school
                continue
            # Standalone parenthetical line: (Alteration) or (Conjuration/Summoning) Reversible
            # Also handles compound: (Abjuration, Evocation)
            clean = stripped.replace("*", "").strip()
            if clean.startswith("("):
                inner_end = clean.find(")")
                if inner_end > 0:
                    inner = clean[1:inner_end].strip()
                    # Check all parts (comma or slash separated)
                    parts = [p.strip().lower() for p in inner.replace("/", ",").split(",")]
                    if any(p in known_schools for p in parts):
                        return inner
        # Fallback: School: field
        for line in raw_content.split("\n"):
            stripped = line.strip()
            if stripped.lower().startswith("school:"):
                return stripped[7:].strip() or None
        return None

    def _extract_field_from_raw(raw_content: str, field_name: str) -> str | None:
        """Extract a metadata field value from raw content (case-insensitive).
        Handles field at start of line or smashed mid-line (e.g. 'Reversible Sphere: Sun')."""
        field_lower = field_name.lower() + ":"
        for line in raw_content.split("\n"):
            line_lower = line.strip().lower()
            if field_lower in line_lower:
                # Find the field and extract value after it
                idx = line_lower.index(field_lower)
                value = line.strip()[idx + len(field_name) + 1:].strip()
                return value if value else None
        return None

    def flush():
        nonlocal current_content, current_school, current_sphere
        if current_content and current_toc:
            # In whitelist sections, skip entries with no title — these are
            # section/level headings, not actual entries
            if _is_whitelist_section(current_toc, config) and not current_entry:
                current_content = []
                current_school = None
                current_sphere = None
                return
            raw_content = "\n".join(current_content).strip()

            # Extract school/sphere from raw content, fall back to captured stripped lines
            school = _extract_school_from_raw(raw_content) or current_school
            sphere = _extract_field_from_raw(raw_content, "Sphere") or current_sphere

            content = raw_content
            if config:
                content = _clean_entry_content(content, config)

            # Strip the entry's own heading line from content (it's metadata, not body)
            if current_entry:
                lines = content.split("\n")
                cleaned = []
                for cl in lines:
                    stripped = cl.strip().lstrip("#").strip()
                    stripped = re.sub(r"\*+", "", stripped).strip()
                    if stripped == current_entry:
                        continue
                    cleaned.append(cl)
                content = "\n".join(cleaned).strip()

            # Strip parenthetical chapter cross-references using ToC titles
            # e.g. "(chapter 9)" or "(Chapter 14: Time and Movement)"
            for sec in _toc_sections:
                if not sec.get("is_chapter"):
                    continue
                sec_title = sec.get("title", "")
                # Strip "(Chapter N)" using the part before ":"
                ch_prefix = sec_title.split(":")[0].strip()
                if ch_prefix:
                    content = content.replace(f"({ch_prefix})", "")
                    content = content.replace(f"({ch_prefix.lower()})", "")
                # Strip full "(Chapter N: Title)"
                content = content.replace(f"({sec_title})", "")

            min_content = config.get("ingestion", {}).get("min_entry_content", 10) if config else 10
            if content and len(content) > min_content:
                entries.append({
                    "toc_entry": current_toc,
                    "section_title": current_sub_section or current_section or current_toc.get("title", "").split(":", 1)[-1].strip(),
                    "entry_title": current_entry,
                    "content": content,
                    "school": school,
                    "sphere": sphere,
                    "spell_class": current_spell_class,
                    "spell_level": current_spell_level,
                    "page_numbers": [current_page],
                })
        current_content = []
        current_school = None
        current_sphere = None

    lines = markdown.split("\n")
    char_pos = 0

    for line in lines:
        h_match = re.match(r"^(#{1,4})\s+(.+)", line)

        if h_match:
            level = len(h_match.group(1))
            heading = h_match.group(2).strip()
            clean_heading = re.sub(r"\*+", "", heading).strip()
            match_name = re.sub(r"\s*\([\w/,\s]+\)\s*$", "", clean_heading).strip()

            # Update chapter from heading-chapter map
            if char_pos in heading_chapter_map:
                hc = heading_chapter_map[char_pos]
                new_toc = hc["toc_entry"]
                if new_toc != current_toc:
                    current_toc = new_toc
                    current_spell_class = _toc_spell_class(current_toc)
                    if current_spell_class and current_spell_level is None:
                        current_spell_level = 1
                    elif not current_spell_class:
                        current_spell_level = None
                current_page = hc["page"]
                current_printed_page = hc.get("printed_page", current_page)

            if level <= 2:
                # Skip duplicate section headings from Marker page-boundary re-renders
                # e.g. "## Priest Spells" repeated at top of new page mid-entry
                if current_section and clean_heading.lower() == current_section.lower():
                    pass  # same section -- keep accumulating current entry
                elif not _is_valid_section_heading(clean_heading, _toc_sections, config or {}):
                    # Marker artifact: garbage H1/H2 heading from page running header
                    # (e.g. "# The", "# Good.", "# Player") — treat as content, not section
                    current_content.append(line)
                elif current_entry and current_content and config and _has_metadata_but_no_description("\n".join(current_content), config):
                    # Current entry has metadata but no description yet -- the description
                    # likely follows this section heading (Marker inserted it mid-entry).
                    # Update section but don't flush the entry.
                    current_section = clean_heading
                else:
                    flush()
                    current_section = clean_heading
                    current_sub_section = None
                    current_spell_level = None
                    # Inherit spell_class from ToC (don't re-derive from heading text)
                    # The ToC is the authority; section headings are just page re-renders
                    if current_spell_class:
                        current_spell_level = 1
                    current_entry = None
                    current_content = [line]
            else:
                # Check if this heading matches a sub-section pattern from config
                # (e.g. "First-Level Spells") — updates section_title, not an entry
                sub_patterns = config.get("section_parsing", {}) if config else {}
                is_sub_section = False
                for sec_key, sec_cfg in sub_patterns.items():
                    sub_pat = sec_cfg.get("sub_section_pattern", "")
                    if sub_pat and re.match(sub_pat, clean_heading, re.IGNORECASE):
                        is_sub_section = True
                        break

                if is_sub_section:
                    flush()
                    current_sub_section = clean_heading
                    # Extract level from sub-section heading
                    sub_lower = clean_heading.lower()
                    current_spell_level = None
                    for word, level in level_mapping.items():
                        if word in sub_lower:
                            current_spell_level = level
                            break
                    current_entry = None
                    current_content = [line]
                else:
                    # Skip H3/H4 headings that match a chapter-level ToC title —
                    # these are cross-references in content, not entry boundaries
                    is_chapter_ref = False
                    for sec in _toc_sections:
                        if not sec.get("is_chapter"):
                            continue
                        sec_title = sec.get("title", "")
                        sec_desc = sec_title.split(":", 1)[-1].strip().lower()
                        if match_name.lower() == sec_title.lower() or match_name.lower() == sec_desc:
                            is_chapter_ref = True
                            break

                    if is_chapter_ref:
                        current_content.append(line)
                    else:
                        # In whitelist sections, only create entries for known headings
                        # In non-whitelist sections, every H3/H4 heading creates a new entry
                        in_whitelist = _is_whitelist_section(current_toc, config)
                        if in_whitelist and known_entries and match_name.lower() not in known_entries:
                            current_content.append(line)
                        else:
                            flush()  # resets current_school/sphere
                            current_entry = match_name
                            current_content = [line]
        else:
            stripped = line.strip()
            if re.match(r"^!\[.*\]\(.*\)$", stripped):
                pass
            elif config and stripped and _should_strip_line(stripped, config):
                # Capture school from stripped parenthetical lines before discarding
                clean = stripped.replace("*", "").strip()
                if clean.startswith("(") and ")" in clean:
                    inner = clean[1:clean.index(")")].strip()
                    # Remove "Reversible" suffix
                    inner = inner.replace("Reversible", "").strip().rstrip(",").strip()
                    if inner and current_school is None:
                        current_school = inner
            else:
                # Check for entry anchors, ToC sub-section titles, and inline patterns.
                # Priority: config entry_anchors > ToC sub-sections > inline patterns
                anchor_matched = False

                # ToC sub-section auto-anchors: if a non-heading line starts with
                # a ToC sub-section title for the current chapter, near the expected
                # page, treat it as an entry boundary. Marker often renders these as
                # bold text instead of headings.
                if not anchor_matched and current_toc and stripped:
                    clean_stripped = stripped.replace("*", "").strip()
                    for toc_sub in _toc_sections:
                        if toc_sub.get("is_chapter") or toc_sub.get("is_excluded") or toc_sub.get("is_table"):
                            continue
                        if toc_sub.get("parent_title") != current_toc.get("title"):
                            continue
                        sub_title = toc_sub.get("title", "")
                        if not sub_title:
                            continue
                        # Check if line starts with the sub-section title
                        if clean_stripped.lower().startswith(sub_title.lower()):
                            # Verify we're near the expected printed page (within 2 pages)
                            expected_page = toc_sub.get("page_start", 0)
                            if expected_page and abs(current_printed_page - expected_page) <= 2:
                                flush()
                                current_entry = sub_title
                                current_content = [line]
                                anchor_matched = True
                                break

                if config and current_toc and stripped and not anchor_matched:
                    toc_title = current_toc.get("title", "")
                    for sec_key, sec_cfg in (config.get("section_parsing", {}) or {}).items():
                        if sec_key.lower() not in toc_title.lower():
                            continue

                        # Check entry anchors first
                        anchors = sec_cfg.get("entry_anchors", [])
                        name_map = sec_cfg.get("anchor_name_map", {})
                        for anchor in anchors:
                            if stripped.startswith(anchor) or stripped.startswith(f"**{anchor}"):
                                flush()
                                current_entry = name_map.get(anchor, anchor)
                                current_content = [line]
                                anchor_matched = True
                                break
                        if anchor_matched:
                            break

                        # Check inline entry pattern
                        inline_pat = sec_cfg.get("inline_entry_pattern")
                        if not inline_pat:
                            break
                        m = re.match(inline_pat, stripped)
                        if m:
                            entry_name = m.group(1).strip().rstrip(":")
                            if len(entry_name) >= 3 and entry_name[0].isupper():
                                flush()
                                current_entry = entry_name
                                current_content = [line]
                                anchor_matched = True
                        break
                if not anchor_matched:
                    current_content.append(line)

        char_pos += len(line) + 1

    flush()

    if config:
        entries = _merge_orphan_entries(entries, config)

    _log(f"  Entries: {len(entries)}")
    return entries


# ── Page position mapping ────────────────────────────────────────

def _build_page_position_map(
    markdown: str,
    page_texts: list[str],
    page_printed: dict[int, int],
    total_pages: int,
    config: dict = None,
) -> list[tuple[int, int]]:
    """Build a map of markdown positions to PDF page indices.

    For each PDF page, finds a unique text snippet from that page in the Marker
    markdown. Returns sorted list of (markdown_position, page_idx) anchors.

    This is the ground truth for locating content in the markdown by page."""
    ingestion = config.get("ingestion", {}) if config else {}
    snippet_lengths = ingestion.get("anchor_snippet_lengths", [40, 30, 20])
    max_lines = ingestion.get("anchor_max_lines", 10)

    md_lower = markdown.lower()
    positions = []
    last_found_pos = 0  # search forward from last found position

    # Process pages in order — search forward from last found position
    # This prevents false matches where a late page's text appears early
    for snippet_len in snippet_lengths:
        anchored_pages = {p for _, p in positions}
        for page_idx in range(total_pages):
            if page_idx in anchored_pages:
                continue
            text = page_texts[page_idx].lower()
            lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 10]

            found = False
            for line in lines[1:max_lines]:
                clean = line.replace("-\n", "").replace("\n", " ").strip()
                if len(clean) < snippet_len:
                    continue
                snippet = clean[:snippet_len]
                # Search forward from last found position (pages are roughly in order)
                pos = md_lower.find(snippet, max(0, last_found_pos - 5000))
                if pos >= 0:
                    positions.append((pos, page_idx))
                    last_found_pos = pos
                    found = True
                    break
            if not found and snippet_len == snippet_lengths[0]:
                # First pass miss — don't advance last_found_pos
                pass

    positions.sort()
    return positions


def _page_at_position(md_pos: int, page_anchors: list[tuple[int, int]]) -> int | None:
    """Given a position in the markdown, find which page it's on by interpolating
    between the nearest page anchors.
    Returns None for positions before the first anchor (front matter)."""
    if not page_anchors:
        return None

    # Content before the first page anchor is front matter -- can't be reliably mapped
    if md_pos < page_anchors[0][0]:
        return None

    # Binary search for the anchor just before this position
    lo, hi = 0, len(page_anchors) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if page_anchors[mid][0] <= md_pos:
            lo = mid
        else:
            hi = mid - 1

    return page_anchors[lo][1]


def _page_to_toc_section(printed_page: int, sections: list[dict]) -> dict | None:
    """Look up which ToC section a printed page belongs to."""
    for section in sections:
        if section["page_start"] <= printed_page <= section["page_end"]:
            return section
    return None


# ── Heading-chapter mapping ──────────────────────────────────────

def build_heading_chapter_map(
    markdown: str,
    toc_sections: list[dict],
    page_texts: list[str],
    page_printed: dict[int, int],
    total_pages: int,
    config: dict = None,
) -> dict[int, dict]:
    """Map heading positions in markdown to ToC chapters using page positions.

    1. Build page-position anchors: find unique text from each PDF page in markdown
    2. For each heading, interpolate which page it's on from nearest anchor
    3. Look up the ToC section by printed page number

    No text matching against ToC titles. Page positions are ground truth.

    Returns {char_position_in_markdown: {"toc_entry": section_dict, "page": page_idx}}."""
    included = [s for s in toc_sections if not s["is_excluded"]]
    if not included:
        return {}

    # Build page-position map from PDF content -> markdown positions
    page_anchors = _build_page_position_map(markdown, page_texts, page_printed, total_pages, config)
    _log(f"  Page anchors: {len(page_anchors)}/{total_pages} pages located in markdown")

    heading_chapters = {}
    mapped = 0
    unmapped = 0

    for m in re.finditer(r"^(#{1,4})\s+(.+)", markdown, re.MULTILINE):
        heading = re.sub(r"\*+", "", m.group(2)).strip()
        if len(heading) < 3:
            continue

        # Find which page this heading is on via position interpolation
        page_idx = _page_at_position(m.start(), page_anchors)
        if page_idx is None:
            unmapped += 1
            continue

        printed = page_printed.get(page_idx, page_idx)
        section = _page_to_toc_section(printed, included)
        if section:
            heading_chapters[m.start()] = {"toc_entry": section, "page": page_idx, "printed_page": printed}
            mapped += 1
        else:
            unmapped += 1

    _log(f"  Heading-chapter map: {mapped} mapped, {unmapped} unmapped (page-based)")
    return heading_chapters


# ── Sub-heading collection ───────────────────────────────────────

def collect_sub_headings(entries: list[dict], toc_sections: list[dict], config: dict) -> None:
    """Collect entry titles per ToC section for query routing."""
    max_subs = config.get("ingestion", {}).get("max_sub_headings_per_section", 50)
    section_headings = {}
    for entry in entries:
        title = entry["toc_entry"]["title"]
        if title not in section_headings:
            section_headings[title] = []
        et = entry.get("entry_title") or entry.get("section_title")
        if et and et not in section_headings[title]:
            section_headings[title].append(et)

    for section in toc_sections:
        if section["is_excluded"]:
            continue
        section["sub_headings"] = section_headings.get(section["title"], [])[:max_subs]

    total = sum(len(s["sub_headings"]) for s in toc_sections if not s["is_excluded"])
    _log(f"  Sub-headings: {total} collected")


# ── Chunking ─────────────────────────────────────────────────────

def _split_preserving_tables(content: str) -> list[str]:
    """Split content into paragraph blocks, keeping table rows together.

    Tables (contiguous lines starting with '|') are kept as single blocks
    even if separated by blank lines within the table structure."""
    raw_blocks = content.split("\n\n")
    merged = []
    i = 0
    while i < len(raw_blocks):
        block = raw_blocks[i]
        # If this block contains table rows, merge with adjacent table blocks
        if any(line.strip().startswith("|") for line in block.split("\n") if line.strip()):
            while i + 1 < len(raw_blocks):
                next_block = raw_blocks[i + 1]
                next_lines = [l.strip() for l in next_block.split("\n") if l.strip()]
                if next_lines and next_lines[0].startswith("|"):
                    block = block + "\n\n" + next_block
                    i += 1
                else:
                    break
        merged.append(block)
        i += 1
    return merged


def chunk_entries(entries: list[dict], config: dict) -> list[dict]:
    chunking = config.get("chunking", {})
    max_chars = chunking.get("max_chars", 800)
    overlap = chunking.get("overlap", 200)

    chunks = []
    for entry in entries:
        content = entry["content"]
        toc = entry["toc_entry"]
        page_str = ",".join(str(p) for p in entry["page_numbers"])

        if len(content) <= max_chars:
            chunks.append({
                "toc_entry": toc, "section_title": entry["section_title"],
                "entry_title": entry["entry_title"], "content": content,
                "page_numbers": page_str, "chunk_type": "content",
            })
        else:
            paragraphs = _split_preserving_tables(content)
            current = ""
            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    chunks.append({
                        "toc_entry": toc, "section_title": entry["section_title"],
                        "entry_title": entry["entry_title"],
                        "content": current.strip(), "page_numbers": page_str,
                        "chunk_type": "content",
                    })
                    # Don't overlap into table blocks — overlap only from text content
                    last_text = current.strip()
                    if overlap > 0 and not last_text.rstrip().endswith("|"):
                        overlap_text = last_text[-overlap:]
                    else:
                        overlap_text = ""
                    current = overlap_text + "\n\n" + para if overlap_text else para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append({
                    "toc_entry": toc, "section_title": entry["section_title"],
                    "entry_title": entry["entry_title"],
                    "content": current.strip(), "page_numbers": page_str,
                    "chunk_type": "content",
                })

    _log(f"  Chunks: {len(chunks)}")
    return chunks


# ── Watermark detection ──────────────────────────────────────────

def _detect_watermarks(page_texts: list[str], total_pages: int, threshold: float = 0.3) -> set[str]:
    """Detect watermark lines from page text."""
    line_counts = {}
    for text in page_texts:
        seen = set()
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 2 and stripped not in seen:
                seen.add(stripped)
                line_counts[stripped] = line_counts.get(stripped, 0) + 1
    min_count = max(int(total_pages * threshold), 3)
    watermarks = {line for line, count in line_counts.items() if count >= min_count}
    if watermarks:
        _log(f"  Watermarks: {len(watermarks)} patterns detected")
    return watermarks
