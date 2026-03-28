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


def _case_insensitive_replace(text: str, old: str, new: str) -> str:
    """Replace all occurrences of ``old`` in ``text`` with ``new``, ignoring case.

    Uses string operations only (no regex) per project rules.
    """
    if not old:
        return text
    lower_text = text.lower()
    lower_old = old.lower()
    result_parts: list[str] = []
    start = 0
    while True:
        idx = lower_text.find(lower_old, start)
        if idx == -1:
            result_parts.append(text[start:])
            break
        result_parts.append(text[start:idx])
        result_parts.append(new)
        start = idx + len(old)
    return "".join(result_parts)


# ── Config ───────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        elif k in result and isinstance(result[k], list) and isinstance(v, list):
            result[k] = result[k] + v
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


# ── Page-based entry building ────────────────────────────────────


def build_entries_from_pages(
    toc_all: list[dict],
    page_texts: dict[int, str],
    spell_list: list[dict],
    authority_entries: list[dict],
    config: dict,
    watermarks: set[str] = None,
) -> list[dict]:
    """Build entries by slicing pymupdf page_texts using ToC page ranges.

    No Marker markdown, no fuzzy heading matching. The ToC gives us exact page
    ranges for every chapter and sub-section. pymupdf gives us the text for
    every page. We just slice and assemble.

    For sub-sections sharing the same page, we split by finding the section
    title text within the concatenated page content, in ToC order.

    Spells and authority entries are matched within their parent section's
    page range using the extended ToC (spell names sorted by level).
    """
    from rapidfuzz import fuzz

    section_cfg = config.get("section_parsing", {}) if config else {}
    level_mapping = config.get("spell_level_mapping", {}) if config else {}
    min_content = config.get("ingestion", {}).get("min_entry_content", 10) if config else 10

    def _rejoin_page_text(text: str) -> str:
        """Clean pymupdf page text: rejoin hyphenated words and soft line breaks."""
        if not text:
            return text
        lines = text.split("\n")
        result = []
        i = 0
        while i < len(lines):
            line = lines[i]
            # Rejoin hyphenated word continuations: "Constitu-" + "tion," -> "Constitution,"
            while line.rstrip().endswith("-") and i + 1 < len(lines):
                next_line = lines[i + 1]
                next_stripped = next_line.lstrip()
                if next_stripped and next_stripped[0].islower():
                    line = line.rstrip()[:-1] + next_stripped
                    i += 1
                else:
                    break
            result.append(line)
            i += 1
        return "\n".join(result)

    # Strip watermarks and rejoin hyphenated lines
    clean_pages = {}
    for pnum, text in page_texts.items():
        if watermarks:
            lines = text.split("\n")
            lines = [l for l in lines if l.strip() not in watermarks]
            text = "\n".join(lines)
        clean_pages[pnum] = _rejoin_page_text(text)

    def _get_page_range_text(page_start: int, page_end: int) -> tuple:
        """Concatenate page texts for a range of printed page numbers.
        Returns (text, page_offsets) where page_offsets maps page_num -> char offset."""
        parts = []
        page_offsets = {}
        offset = 0
        for p in range(page_start, page_end + 1):
            if p in clean_pages:
                page_offsets[p] = offset
                parts.append(clean_pages[p])
                offset += len(clean_pages[p]) + 2  # +2 for \n\n separator
        return "\n\n".join(parts), page_offsets

    def _normalize(s: str) -> str:
        """Collapse all whitespace to single spaces for matching."""
        return " ".join(s.split())

    def _build_norm_map(text: str):
        """Build normalized text with char mapping back to original positions."""
        text_chars = []  # (original_idx, char)
        i = 0
        in_space = False
        while i < len(text):
            if text[i] in " \t\n\r":
                if not in_space and text_chars:
                    text_chars.append((i, " "))
                    in_space = True
            else:
                text_chars.append((i, text[i]))
                in_space = False
            i += 1
        norm_text = "".join(c for _, c in text_chars).lower()
        return norm_text, text_chars

    def _is_para_start(text: str, pos: int) -> bool:
        """Check if position is at the start of a paragraph (after blank line or start of text)."""
        if pos == 0:
            return True
        # Look backwards for double newline or start of text
        before = text[max(0, pos - 3):pos]
        return "\n\n" in before or "\n \n" in text[max(0, pos - 5):pos]

    def _find_title_in_text(title: str, text: str, search_from: int = 0,
                            prefer_para_start: bool = True) -> int:
        """Find a section title in text using normalized matching.

        Handles line breaks mid-title and OCR variations.
        When prefer_para_start is True and multiple matches exist,
        prefers matches at paragraph boundaries (after blank lines).
        Returns char position in original text, or -1.
        """
        title_norm = _normalize(title).lower()
        if not title_norm:
            return -1

        norm_text, text_chars = _build_norm_map(text)
        norm_from = 0

        if search_from > 0:
            for ni, (oi, _) in enumerate(text_chars):
                if oi >= search_from:
                    norm_from = ni
                    break

        # Find all normalized matches
        matches = []
        pos = norm_from
        while True:
            pos = norm_text.find(title_norm, pos)
            if pos < 0:
                break
            orig_pos = text_chars[pos][0]
            matches.append(orig_pos)
            pos += 1

        if not matches:
            # Fuzzy fallback for shorter titles
            if len(title_norm) > 60:
                return -1
            window = len(title_norm) + 5
            best_score = 0
            best_pos = -1
            for wi in range(norm_from, len(norm_text) - len(title_norm) + 5):
                chunk = norm_text[wi:wi + window]
                score = fuzz.ratio(chunk, title_norm)
                if score > best_score and score >= 80:
                    best_score = score
                    best_pos = wi
            if best_pos >= 0:
                return text_chars[best_pos][0]
            return -1

        if len(matches) == 1 or not prefer_para_start:
            return matches[0]

        # Multiple matches — prefer one at paragraph start
        for m in matches:
            if _is_para_start(text, m):
                return m

        # No paragraph-start match, return first
        return matches[0]

    def _count_occurrences(title: str, text: str) -> int:
        """Count how many times title appears in text (normalized)."""
        t = _normalize(title).lower()
        txt = _normalize(text).lower()
        if not t:
            return 9999
        count = 0
        pos = 0
        while True:
            pos = txt.find(t, pos)
            if pos < 0:
                break
            count += 1
            pos += len(t)
        return count

    def _split_sections_in_text(text: str, sections: list[dict],
                               page_offsets: dict = None) -> tuple:
        """Split text into sections by finding each title in order.

        page_offsets: {printed_page_num: char_offset_in_text} — when provided,
        uses each section's page_start to narrow the search to the right page
        area, avoiding false matches on earlier pages.

        Returns (intro_str, [(section_dict, content_str), ...]).
        intro_str is text before the first matched section (may be empty)."""

        if not sections:
            return text.strip(), []

        matched = {}  # idx -> char_pos

        # Match each section, using page_start to guide the search area
        for idx, sec in enumerate(sections):
            title = sec["title"]
            page_start = sec.get("page_start", 0)

            # Start after previous matched section
            search_from = 0
            for prev_idx in range(idx - 1, -1, -1):
                if prev_idx in matched:
                    prev_end = matched[prev_idx] + len(sections[prev_idx]["title"])
                    search_from = prev_end
                    break

            # If still on an earlier page than this section's page_start,
            # jump forward to the section's page (avoids false matches)
            if page_offsets and page_start in page_offsets:
                page_offset = page_offsets[page_start]
                if search_from < page_offset:
                    search_from = page_offset

            pos = _find_title_in_text(title, text, search_from,
                                       prefer_para_start=True)
            if pos >= 0:
                matched[idx] = pos

        # Sort matched by position
        ordered = sorted(matched.items(), key=lambda x: x[1])

        # Intro: text before first match
        intro = ""
        if ordered:
            intro = text[:ordered[0][1]].strip()
        elif text.strip():
            intro = text.strip()

        # Extract content between consecutive matches
        results = []
        for i, (idx, pos) in enumerate(ordered):
            if i + 1 < len(ordered):
                end = ordered[i + 1][1]
            else:
                end = len(text)
            content = text[pos:end].strip()
            if len(content) >= min_content:
                results.append((sections[idx], content))

        return intro, results

    # Get chapters (non-excluded, in page order)
    chapters = sorted(
        [t for t in toc_all if t.get("is_chapter") and not t.get("is_excluded")],
        key=lambda t: t.get("page_start", 0),
    )

    entries = []

    for ch in chapters:
        ch_title = ch["title"]
        ch_start = ch.get("page_start", 0)
        ch_end = ch.get("page_end", 9999)

        # Determine entry_mode from config
        entry_mode = "toc"
        matched_cfg = None
        for sec_key, sec_cfg in section_cfg.items():
            if sec_key.lower() in ch_title.lower():
                entry_mode = sec_cfg.get("entry_mode", "toc")
                matched_cfg = sec_cfg
                break

        # Get non-excluded sub-sections, in page order
        subs = sorted(
            [t for t in toc_all
             if t.get("parent_title") == ch_title
             and not t.get("is_excluded")
             and not t.get("is_table")],
            key=lambda t: t.get("page_start", 0),
        )

        # Also check if any sub has its own section_parsing config
        sub_cfgs = {}
        for sub in subs:
            for sec_key, sec_cfg in section_cfg.items():
                if sec_key.lower() in sub.get("title", "").lower():
                    sub_cfgs[sub["title"]] = sec_cfg
                    break

        # Get chapter text with page offsets
        ch_text, ch_page_offsets = _get_page_range_text(ch_start, ch_end)
        if not ch_text.strip():
            continue

        if entry_mode == "per_list" and matched_cfg:
            list_source = matched_cfg.get("list_source", "")
            list_filter = matched_cfg.get("list_filter_type", "")
            sub_pat = matched_cfg.get("sub_section_pattern", "")

            # Determine spell class
            spell_class = None
            if "wizard" in ch_title.lower():
                spell_class = "wizard"
            elif "priest" in ch_title.lower():
                spell_class = "priest"

            if list_source == "spell_list_entries":
                # Group spells by level
                spells_by_level = {}
                for s in spell_list:
                    sc = (s.get("entry_class") or "").lower()
                    if spell_class and sc != spell_class:
                        continue
                    lvl = int(s.get("entry_level") or 0)
                    spells_by_level.setdefault(lvl, []).append(s)
                for lvl in spells_by_level:
                    spells_by_level[lvl].sort(
                        key=lambda s: (s.get("entry_name") or "").lower()
                    )

                # Find level sub-sections first, then spells within each
                level_subs = []
                for sub in subs:
                    if sub_pat and re.match(sub_pat, sub["title"], re.IGNORECASE):
                        matched_level = None
                        for word, lvl in level_mapping.items():
                            if word.lower() in sub["title"].lower():
                                matched_level = lvl
                                break
                        if matched_level is not None:
                            level_subs.append((sub, matched_level))

                for level_sub, lvl in level_subs:
                    ls_start = level_sub.get("page_start", ch_start)
                    ls_end = level_sub.get("page_end", ch_end)
                    level_text, level_offsets = _get_page_range_text(ls_start, ls_end)

                    spells = spells_by_level.get(lvl, [])
                    # Build section list from spell names, with page_start for offset guidance
                    spell_sections = [
                        {"title": s.get("entry_name", ""), "spell": s,
                         "page_start": ls_start}
                        for s in spells
                    ]
                    _intro, found = _split_sections_in_text(level_text, spell_sections, level_offsets)
                    for sec, content in found:
                        spell = sec.get("spell", {})
                        content = _clean_entry_content(content, config)
                        if len(content) < min_content:
                            continue
                        entries.append({
                            "toc_entry": ch,
                            "section_title": sec["title"],
                            "entry_title": sec["title"],
                            "content": content,
                            "school": spell.get("school"),
                            "sphere": spell.get("sphere"),
                            "spell_class": spell_class,
                            "spell_level": lvl,
                            "page_numbers": [ls_start],
                        })

            elif list_source == "authority_table_entries":
                # Authority entries (proficiencies etc) within the section's page range
                items = sorted(
                    [a for a in authority_entries
                     if not list_filter or a.get("entry_type", "") == list_filter],
                    key=lambda a: a.get("entry_name", "").lower(),
                )
                item_sections = [{"title": a.get("entry_name", ""), "authority": a} for a in items]
                _intro, found = _split_sections_in_text(ch_text, item_sections, ch_page_offsets)
                for sec, content in found:
                    content = _clean_entry_content(content, config)
                    if len(content) < min_content:
                        continue
                    entries.append({
                        "toc_entry": ch,
                        "section_title": sec["title"],
                        "entry_title": sec["title"],
                        "content": content,
                        "school": None, "sphere": None,
                        "spell_class": None, "spell_level": None,
                        "page_numbers": [ch_start],
                    })

        else:
            # toc mode: split by sub-sections
            if subs:
                # Check if any sub has its own per_list config
                has_per_list_sub = False
                regular_subs = []
                for sub in subs:
                    sub_cfg = sub_cfgs.get(sub["title"])
                    if sub_cfg and sub_cfg.get("entry_mode") == "per_list":
                        has_per_list_sub = True
                        # Handle this sub's per_list entries
                        sub_start = sub.get("page_start", ch_start)
                        sub_end = sub.get("page_end", ch_end)
                        sub_text, sub_offsets = _get_page_range_text(sub_start, sub_end)
                        list_source = sub_cfg.get("list_source", "")
                        list_filter = sub_cfg.get("list_filter_type", "")
                        if list_source == "authority_table_entries":
                            items = sorted(
                                [a for a in authority_entries
                                 if not list_filter or a.get("entry_type", "") == list_filter],
                                key=lambda a: a.get("entry_name", "").lower(),
                            )
                            item_sections = [{"title": a.get("entry_name", ""), "page_start": sub_start} for a in items]
                            _intro, found = _split_sections_in_text(sub_text, item_sections, sub_offsets)
                            for sec, content in found:
                                content = _clean_entry_content(content, config)
                                if len(content) < min_content:
                                    continue
                                entries.append({
                                    "toc_entry": ch,
                                    "section_title": sec["title"],
                                    "entry_title": sec["title"],
                                    "content": content,
                                    "school": None, "sphere": None,
                                    "spell_class": None, "spell_level": None,
                                    "page_numbers": [sub_start],
                                })
                    else:
                        regular_subs.append(sub)

                # Split remaining subs from chapter text
                intro, found = _split_sections_in_text(ch_text, regular_subs, ch_page_offsets)

                # Chapter intro: content before the first sub-section
                if intro:
                    intro = _clean_entry_content(intro, config)
                    if len(intro) >= min_content:
                        entries.append({
                            "toc_entry": ch,
                            "section_title": ch_title,
                            "entry_title": ch_title,
                            "content": intro,
                            "school": None, "sphere": None,
                            "spell_class": None, "spell_level": None,
                            "page_numbers": [ch_start],
                        })

                for sec, content in found:
                    content = _clean_entry_content(content, config)
                    if len(content) < min_content:
                        continue
                    entries.append({
                        "toc_entry": ch,
                        "section_title": sec["title"],
                        "entry_title": sec["title"],
                        "content": content,
                        "school": None, "sphere": None,
                        "spell_class": None, "spell_level": None,
                        "page_numbers": [sec.get("page_start", ch_start)],
                    })
            else:
                # No sub-sections — whole chapter is one entry
                content = _clean_entry_content(ch_text, config)
                if len(content) >= min_content:
                    entries.append({
                        "toc_entry": ch,
                        "section_title": ch_title,
                        "entry_title": None,
                        "content": content,
                        "school": None, "sphere": None,
                        "spell_class": None, "spell_level": None,
                        "page_numbers": [ch_start],
                    })

    _log(f"  Entries from pages: {len(entries)}")
    return entries


# ── Stream-based entry building (legacy, kept for reference) ─────


def build_extended_toc(
    toc_all: list[dict],
    spell_list: list[dict],
    authority_entries: list[dict],
    config: dict,
) -> list[dict]:
    """Extend the real ToC with entries from config, spell_list, and authority tables.

    Walks chapters in page order. For each chapter, checks section_parsing config:
    - per_list (spell_list_entries): group spells by level, inject alphabetically
      under the matching level sub-section heading
    - per_list (authority_table_entries): inject authority entries alphabetically
    - per_anchor: inject entry_anchors from config as leaf nodes
    - entry_anchors without entry_mode: inject anchors (e.g. "The Real Basics")
    - toc (default): sub-sections from ToC are leaves

    Returns a flat ordered list ready for stream matching.
    """
    section_cfg = config.get("section_parsing", {}) if config else {}
    level_mapping = config.get("spell_level_mapping", {}) if config else {}

    chapters = [t for t in toc_all if t.get("is_chapter") and not t.get("is_excluded")]
    extended = []

    for ch in chapters:
        ch_title = ch["title"]

        # Add the chapter node (never a leaf)
        extended.append({
            **ch, "is_leaf": False, "entry_mode": "toc",
            "spell_class": None, "spell_level": None,
            "school": None, "sphere": None, "source": "toc",
        })

        # Find matching section_parsing config for this chapter
        # Match on chapter title containing the config key OR sub-section parent
        entry_mode = "toc"
        matched_cfg = None
        for sec_key, sec_cfg in section_cfg.items():
            if sec_key.lower() in ch_title.lower():
                entry_mode = sec_cfg.get("entry_mode", "toc")
                matched_cfg = sec_cfg
                break

        # Sub-sections from real ToC, page-ordered
        subs = sorted(
            [t for t in toc_all if t.get("parent_title") == ch_title and not t.get("is_excluded")],
            key=lambda s: s.get("page_start", 0),
        )

        # Also check if any sub-section has its own section_parsing config
        # (e.g. "Nonweapon Proficiency Descriptions" is a sub-section of Ch5)
        sub_configs = {}
        for sub in subs:
            for sec_key, sec_cfg in section_cfg.items():
                if sec_key.lower() in sub.get("title", "").lower():
                    sub_configs[sub["title"]] = sec_cfg
                    break

        if entry_mode == "per_list" and matched_cfg:
            _extend_per_list(extended, ch, subs, matched_cfg, spell_list,
                             authority_entries, level_mapping)
        elif entry_mode == "per_anchor" and matched_cfg:
            _extend_per_anchor(extended, ch, subs, matched_cfg)
        elif matched_cfg and "entry_anchors" in matched_cfg:
            # Has anchors but no entry_mode (like "The Real Basics")
            _extend_per_anchor(extended, ch, subs, matched_cfg)
        else:
            # Default toc mode — sub-sections are leaves
            # But check if individual sub-sections have their own config
            for sub in subs:
                sub_cfg = sub_configs.get(sub["title"])
                if sub_cfg and sub_cfg.get("entry_mode") == "per_list":
                    # This sub-section expands into per_list entries
                    extended.append({
                        **sub, "is_leaf": False, "entry_mode": "per_list",
                        "spell_class": None, "spell_level": None,
                        "school": None, "sphere": None, "source": "toc",
                    })
                    _extend_sub_per_list(extended, ch, sub, sub_cfg,
                                         authority_entries)
                elif sub_cfg and "entry_anchors" in sub_cfg:
                    extended.append({
                        **sub, "is_leaf": False, "entry_mode": "per_anchor",
                        "spell_class": None, "spell_level": None,
                        "school": None, "sphere": None, "source": "toc",
                    })
                    _extend_per_anchor(extended, ch, [], sub_cfg)
                else:
                    extended.append({
                        **sub,
                        "is_leaf": not sub.get("is_table", False),
                        "entry_mode": "toc",
                        "spell_class": None, "spell_level": None,
                        "school": None, "sphere": None, "source": "toc",
                    })

    leaf_count = sum(1 for e in extended if e.get("is_leaf"))
    spell_count = sum(1 for e in extended if e.get("source") == "spell_list")
    auth_count = sum(1 for e in extended if e.get("source") == "authority")
    anchor_count = sum(1 for e in extended if e.get("source") == "anchor")
    _log(f"  Extended ToC: {len(extended)} nodes, {leaf_count} leaves "
         f"({spell_count} spells, {auth_count} authority, {anchor_count} anchors)")
    return extended


def _extend_per_list(extended, ch, subs, cfg, spell_list, authority_entries,
                     level_mapping):
    """Extend ToC for a per_list chapter (spells or authority entries)."""
    list_source = cfg.get("list_source", "")
    list_filter = cfg.get("list_filter_type", "")
    sub_pat = cfg.get("sub_section_pattern", "")
    ch_title = ch["title"]

    # Determine spell class from chapter title
    spell_class = None
    ch_lower = ch_title.lower()
    if "wizard" in ch_lower:
        spell_class = "wizard"
    elif "priest" in ch_lower:
        spell_class = "priest"

    if list_source == "spell_list_entries":
        # Group spells by level, sorted alphabetically within each
        spells_by_level = {}
        for s in spell_list:
            sc = (s.get("entry_class") or s.get("spell_class") or "").lower()
            if spell_class and sc != spell_class:
                continue
            lvl = int(s.get("entry_level") or s.get("spell_level") or 0)
            spells_by_level.setdefault(lvl, []).append(s)
        for lvl in spells_by_level:
            spells_by_level[lvl].sort(
                key=lambda s: (s.get("entry_name") or s.get("spell_name") or "").lower()
            )

        for sub in subs:
            is_table = sub.get("is_table", False)
            extended.append({
                **sub, "is_leaf": is_table, "entry_mode": "per_list",
                "spell_class": spell_class, "spell_level": None,
                "school": None, "sphere": None, "source": "toc",
            })
            if is_table:
                continue

            # Check if this sub-section is a level header
            matched_level = None
            if sub_pat:
                sub_title = sub.get("title", "")
                if re.match(sub_pat, sub_title, re.IGNORECASE):
                    for word, lvl in level_mapping.items():
                        if word.lower() in sub_title.lower():
                            matched_level = lvl
                            break

            if matched_level is not None and matched_level in spells_by_level:
                for s in spells_by_level[matched_level]:
                    name = s.get("entry_name") or s.get("spell_name") or ""
                    extended.append(_make_leaf(
                        name, ch_title, sub.get("page_start", 0),
                        sub.get("page_end", 9999),
                        entry_mode="per_list", source="spell_list",
                        spell_class=spell_class, spell_level=matched_level,
                        school=s.get("school"), sphere=s.get("sphere"),
                    ))

    elif list_source == "authority_table_entries":
        for sub in subs:
            extended.append({
                **sub, "is_leaf": sub.get("is_table", False), "entry_mode": "per_list",
                "spell_class": None, "spell_level": None,
                "school": None, "sphere": None, "source": "toc",
            })

        names = sorted(
            [a for a in authority_entries
             if not list_filter or a.get("entry_type", "") == list_filter],
            key=lambda a: a.get("entry_name", "").lower(),
        )
        for a in names:
            extended.append(_make_leaf(
                a.get("entry_name", ""), ch_title,
                ch.get("page_start", 0), ch.get("page_end", 9999),
                entry_mode="per_list", source="authority",
            ))


def _extend_sub_per_list(extended, ch, sub, cfg, authority_entries):
    """Extend ToC for a sub-section that is per_list (e.g. Proficiency Descriptions)."""
    list_source = cfg.get("list_source", "")
    list_filter = cfg.get("list_filter_type", "")
    ch_title = ch["title"]

    if list_source == "authority_table_entries":
        names = sorted(
            [a for a in authority_entries
             if not list_filter or a.get("entry_type", "") == list_filter],
            key=lambda a: a.get("entry_name", "").lower(),
        )
        for a in names:
            extended.append(_make_leaf(
                a.get("entry_name", ""), ch_title,
                sub.get("page_start", 0), sub.get("page_end", 9999),
                entry_mode="per_list", source="authority",
            ))


def _extend_per_anchor(extended, ch, subs, cfg):
    """Extend ToC for a per_anchor section.

    per_anchor was needed when the matcher only searched heading-formatted lines.
    Now that _find_in_stream searches all lines, per_anchor is equivalent to toc
    mode — the ToC sub-sections are the truth. Anchors only matter for entries
    NOT already in the ToC subs (injected as new leaf nodes).
    """
    anchors = cfg.get("entry_anchors", [])
    name_map = cfg.get("anchor_name_map", {})
    ch_title = ch["title"]

    # Add ToC subs as usual
    sub_titles_lower = set()
    for sub in subs:
        sub_titles_lower.add(sub.get("title", "").lower())
        extended.append({
            **sub,
            "is_leaf": not sub.get("is_table", False),
            "entry_mode": "toc",
            "spell_class": None, "spell_level": None,
            "school": None, "sphere": None, "source": "toc",
        })

    # Only add anchors that are NOT already in the ToC subs
    for anchor in anchors:
        raw = anchor.replace("**", "").strip()
        display_name = name_map.get(anchor, raw)
        if raw.lower() not in sub_titles_lower and display_name.lower() not in sub_titles_lower:
            extended.append(_make_leaf(
                display_name, ch_title,
                ch.get("page_start", 0), ch.get("page_end", 9999),
                entry_mode="toc", source="anchor",
                _anchor_text=anchor,
            ))


def _make_leaf(title, parent_title, page_start, page_end,
               entry_mode="toc", source="toc", **kwargs):
    """Create a leaf node for the extended ToC."""
    return {
        "title": title,
        "parent_title": parent_title,
        "is_chapter": False,
        "is_table": False,
        "is_excluded": False,
        "is_leaf": True,
        "page_start": page_start,
        "page_end": page_end,
        "entry_mode": entry_mode,
        "spell_class": kwargs.get("spell_class"),
        "spell_level": kwargs.get("spell_level"),
        "school": kwargs.get("school"),
        "sphere": kwargs.get("sphere"),
        "source": source,
        "_anchor_text": kwargs.get("_anchor_text"),
        "sub_headings": [],
        "tables": [],
    }


def _clean_line_for_matching(text: str) -> str:
    """Strip markdown formatting from a line for fuzzy matching."""
    clean = text.strip()
    while clean.startswith("#"):
        clean = clean[1:]
    clean = clean.replace("**", "").replace("*", "").strip()
    # Remove trailing parenthetical (school names, chapter refs)
    paren = clean.rfind("(")
    if paren > 0 and clean.endswith(")"):
        clean = clean[:paren].strip()
    # Remove "Reversible" suffix
    if clean.lower().endswith("reversible"):
        clean = clean[:clean.lower().rfind("reversible")].strip()
    return clean


def _find_in_stream(lines: list[str], target: str, start: int, end: int,
                    threshold: int = 80) -> int:
    """Find a line in the stream that matches target using fuzzy matching.

    Searches lines[start:end] for a line whose cleaned text is a good fuzzy
    match against target. Only considers lines that look like headings or
    short standalone text (not buried in paragraphs).

    Returns line index or -1.
    """
    from rapidfuzz import fuzz
    target_lower = target.lower().strip()
    if not target_lower:
        return -1

    for i in range(start, min(end, len(lines))):
        raw = lines[i].strip()
        if not raw:
            continue

        clean = _clean_line_for_matching(raw)
        if not clean:
            continue
        clean_lower = clean.lower()

        # Exact match (works on any line length)
        if clean_lower == target_lower:
            return i

        # Check if the target appears at the start of the line (even long lines)
        # Handles "Dwarves are short, stocky..." matching "Dwarves"
        if len(target_lower) >= 4 and clean_lower.startswith(target_lower):
            after = clean_lower[len(target_lower):]
            if not after or not after[0].isalpha():
                # Number guard for startswith too
                t_nums = [w for w in target_lower.split() if w.isdigit()]
                c_nums = [w for w in clean_lower.split() if w.isdigit()]
                if t_nums and c_nums and t_nums != c_nums:
                    continue
                return i

        # Skip remaining checks for very long lines
        if len(clean) > len(target) * 3 + 40:
            continue

        # Fuzzy match — only on lines short enough to be headings
        if len(clean) <= len(target) * 2 + 20:
            score = fuzz.ratio(clean_lower, target_lower)
            if score >= threshold:
                # Guard against numbered near-misses: "Chapter 11" != "Chapter 12"
                # If both contain a number, the numbers must match
                t_nums = [w for w in target_lower.split() if w.isdigit()]
                c_nums = [w for w in clean_lower.split() if w.isdigit()]
                if t_nums and c_nums and t_nums != c_nums:
                    continue
                return i

    return -1


def build_entries_from_stream(
    markdown: str,
    extended_toc: list[dict],
    config: dict,
    watermarks: set[str] = None,
) -> list[dict]:
    """Build entries by walking the markdown stream against the extended ToC.

    Algorithm:
    1. Strip watermark lines
    2. Find chapter boundaries (search all lines, not just headings)
    3. Within each chapter, find leaf nodes in order using fuzzy matching
    4. Extract content between consecutive matched positions

    Returns list of entry dicts ready for silver_entries.
    """
    if not extended_toc:
        return []

    # Apply content_substitutions before splitting — fixes OCR errors for matching
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            markdown = markdown.replace(sub[0], sub[1])

    lines = markdown.split("\n")
    if watermarks:
        lines = [l if l.strip() not in watermarks else "" for l in lines]

    min_content = config.get("ingestion", {}).get("min_entry_content", 10) if config else 10

    # ── Phase 1: Find chapter boundaries ──
    # Chapters use ONLY heading-formatted lines (# or **) with exact number match.
    # This avoids false matches on body text and Marker page header repeats.
    chapters_only = [e for e in extended_toc if e.get("is_chapter")]

    # Index heading-formatted lines only
    heading_lines = []  # (line_idx, clean_text)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or (stripped.startswith("**") and "**" in stripped[2:]):
            clean = _clean_line_for_matching(stripped)
            if clean and len(clean) >= 2:
                heading_lines.append((i, clean))

    def _find_chapter_heading(target: str, start: int) -> int:
        """Find a chapter heading. Only searches heading-formatted lines.
        Requires exact number match for numbered chapters."""
        from rapidfuzz import fuzz
        target_lower = target.lower().strip()
        # Extract numbers from target
        t_nums = [w for w in target_lower.split() if w.isdigit()]

        for h_line, h_clean in heading_lines:
            if h_line < start:
                continue
            h_lower = h_clean.lower()
            # Number guard: if target has numbers, line must have same numbers
            h_nums = [w for w in h_lower.split() if w.isdigit()]
            if t_nums and h_nums and t_nums != h_nums:
                continue

            if h_lower == target_lower:
                return h_line
            if fuzz.ratio(h_lower, target_lower) >= 85:
                return h_line
        return -1

    chapter_start = {}
    search_from = 0
    for ch in chapters_only:
        ch_title = ch["title"]
        # Try descriptive part FIRST — it appears before "Chapter N" page headers
        # in the stream because the actual content heading comes first.
        # "Chapter N" page headers are Marker artifacts that appear mid-chapter.
        # Build candidates most-specific to least, with paren-stripped variants
        # interleaved so "Proficiencies" is tried before "Chapter 5"
        raw_candidates = []
        if ":" in ch_title:
            desc = ch_title.split(":", 1)[-1].strip()
            raw_candidates.append(desc)
            for prefix in ("Player Character ", "PC "):
                if desc.lower().startswith(prefix.lower()):
                    raw_candidates.append(desc[len(prefix):].strip())
            raw_candidates.append(ch_title)
            raw_candidates.append(ch_title.split(":", 1)[0].strip())
        else:
            raw_candidates.append(ch_title)
        # Expand: for each candidate, also try without trailing parens
        candidates = []
        for c in raw_candidates:
            candidates.append(c)
            paren = c.rfind("(")
            if paren > 0:
                stripped = c[:paren].strip()
                if stripped and stripped not in candidates:
                    candidates.append(stripped)

        pos = -1
        matched_cand = ""
        for cand in candidates:
            if len(cand) < 3:
                continue
            pos = _find_chapter_heading(cand, search_from)
            if pos >= 0:
                matched_cand = cand
                break
        if pos >= 0:
            chapter_start[ch_title] = pos
            _log(f"    ch {pos:5d}: [{ch_title}] via [{matched_cand}] -> [{_clean_line_for_matching(lines[pos])}]")
            search_from = pos + 1
        else:
            _log(f"    ch  MISS: [{ch_title}]")

    # Build chapter bounds
    chapter_bounds = {}
    ch_titles = list(chapter_start.keys())
    for i, title in enumerate(ch_titles):
        start = chapter_start[title]
        end = chapter_start[ch_titles[i + 1]] if i + 1 < len(ch_titles) else len(lines)
        chapter_bounds[title] = (start, end)

    _log(f"  Chapter boundaries: {len(chapter_bounds)} of {len(chapters_only)} chapters found")

    # ── Phase 2: Find leaf nodes within chapter ranges ──
    # Rare-first strategy: match unique/long titles first (they have fewer false
    # matches), then fill in common/short titles constrained between neighbors.

    # Collect all leaves with their chapter ranges
    leaf_items = []  # (toc_index, node, ch_start, ch_end)
    current_chapter = None
    ch_s, ch_e = 0, len(lines)
    for toc_idx, node in enumerate(extended_toc):
        if node.get("is_chapter"):
            current_chapter = node["title"]
            bounds = chapter_bounds.get(current_chapter)
            if bounds:
                ch_s, ch_e = bounds
            continue
        if node.get("is_excluded") or node.get("is_table") or not node.get("is_leaf"):
            continue
        leaf_items.append((toc_idx, node, ch_s, ch_e))

    def _occurrence_count(title: str, ch_s: int, ch_e: int) -> int:
        """Count how many lines in the chapter range contain this title.
        Fewer occurrences = more unique = should match first."""
        t_lower = title.lower().strip()
        if not t_lower:
            return 9999
        count = 0
        for i in range(ch_s, min(ch_e, len(lines))):
            if t_lower in lines[i].lower():
                count += 1
        return count

    def _match_leaf(node, search_start, search_end):
        """Try to find a leaf node in the stream. Returns line index or -1."""
        target = node.get("title", "")
        anchor_text = node.get("_anchor_text")

        candidates = [target]
        if ":" in target:
            candidates.append(target.split(":", 1)[-1].strip())
        for c in list(candidates):
            paren = c.rfind("(")
            if paren > 0:
                candidates.append(c[:paren].strip())

        for cand in candidates:
            if len(cand) < 3:
                continue
            pos = _find_in_stream(lines, cand, search_start, search_end, 80)
            if pos >= 0:
                return pos

        if anchor_text:
            for i in range(search_start, min(search_end, len(lines))):
                if anchor_text in lines[i]:
                    return i
        return -1

    # Pass 1: Match rare leaves first (sorted by uniqueness, descending)
    # These anchor the stream positions reliably
    matched = {}  # toc_index -> line_idx
    sorted_by_rarity = sorted(leaf_items, key=lambda x: _occurrence_count(x[1]["title"], x[2], x[3]))

    for toc_idx, node, ch_s, ch_e in sorted_by_rarity:
        pos = _match_leaf(node, ch_s, ch_e)
        if pos >= 0:
            matched[toc_idx] = pos

    rare_count = len(matched)

    # Pass 2: Fill in unmatched leaves, constrained between their matched neighbors
    # For each unmatched leaf, find the nearest matched neighbors in ToC order
    # and search only between those positions
    for toc_idx, node, ch_s, ch_e in leaf_items:
        if toc_idx in matched:
            continue

        # Find nearest matched predecessor in ToC order
        search_start = ch_s
        for prev_idx, prev_node, _, _ in reversed(leaf_items):
            if prev_idx < toc_idx and prev_idx in matched:
                search_start = matched[prev_idx] + 1
                break

        # Find nearest matched successor in ToC order
        search_end = ch_e
        for next_idx, next_node, _, _ in leaf_items:
            if next_idx > toc_idx and next_idx in matched:
                search_end = matched[next_idx]
                break

        if search_start < search_end:
            pos = _match_leaf(node, search_start, search_end)
            if pos >= 0:
                matched[toc_idx] = pos

    # Pass 3: Validate ToC ordering — matched positions must be monotonically
    # increasing by toc_index within each chapter. Re-match violations.
    toc_idx_to_ch = {toc_idx: (ch_s, ch_e) for toc_idx, _, ch_s, ch_e in leaf_items}
    toc_idx_to_node_map = {toc_idx: node for toc_idx, node, _, _ in leaf_items}
    toc_indices = sorted(matched.keys())

    rematched = 0
    for attempt in range(3):  # iterate a few times to settle
        violations = []
        for i in range(len(toc_indices) - 1):
            idx_a, idx_b = toc_indices[i], toc_indices[i + 1]
            # Only check within same chapter
            if toc_idx_to_ch[idx_a][0] != toc_idx_to_ch[idx_b][0]:
                continue
            if matched[idx_a] >= matched[idx_b]:
                # Out of order — re-match the one with more occurrences
                ch_s, ch_e = toc_idx_to_ch[idx_a]
                count_a = _occurrence_count(toc_idx_to_node_map[idx_a]["title"], ch_s, ch_e)
                count_b = _occurrence_count(toc_idx_to_node_map[idx_b]["title"], ch_s, ch_e)
                violations.append(idx_a if count_a >= count_b else idx_b)

        if not violations:
            break

        for bad_idx in set(violations):
            del matched[bad_idx]
            toc_indices = sorted(matched.keys())

        # Re-match removed entries between neighbors
        for bad_idx in set(violations):
            ch_s, ch_e = toc_idx_to_ch[bad_idx]
            search_start = ch_s
            for prev in toc_indices:
                if prev < bad_idx and prev in matched:
                    search_start = matched[prev] + 1
            search_end = ch_e
            for nxt in toc_indices:
                if nxt > bad_idx and nxt in matched:
                    search_end = matched[nxt]
                    break
            if search_start < search_end:
                pos = _match_leaf(toc_idx_to_node_map[bad_idx], search_start, search_end)
                if pos >= 0:
                    matched[bad_idx] = pos
                    rematched += 1
            toc_indices = sorted(matched.keys())

    # Build matched_leaves in document order (sorted by line position)
    matched_leaves = sorted(
        [(line_idx, toc_idx_to_node_map[toc_idx]) for toc_idx, line_idx in matched.items()],
        key=lambda x: x[0],
    )

    _log(f"  Matched leaves: {len(matched_leaves)} of {len(leaf_items)} "
         f"({rare_count} rare, {len(matched_leaves) - rare_count} filled)")

    # ── Phase 3: Extract content between consecutive matched leaves ──
    entries = []
    for i, (line_idx, node) in enumerate(matched_leaves):
        if i + 1 < len(matched_leaves):
            end_line = matched_leaves[i + 1][0]
        else:
            # Last entry — use chapter end
            ch_title = node.get("parent_title") or current_chapter
            bounds = chapter_bounds.get(ch_title)
            end_line = bounds[1] if bounds else len(lines)

        raw_content = "\n".join(lines[line_idx:end_line]).strip()
        content = _clean_entry_content(raw_content, config) if config else raw_content

        if len(content) < min_content:
            continue

        # Find parent chapter
        parent_title = node.get("parent_title", "")
        parent_ch = None
        for ch in chapters_only:
            if ch["title"] == parent_title:
                parent_ch = ch
                break
        toc_entry = parent_ch or node

        entries.append({
            "toc_entry": toc_entry,
            "section_title": node.get("title"),
            "entry_title": node.get("title"),
            "content": content,
            "school": node.get("school"),
            "sphere": node.get("sphere"),
            "spell_class": node.get("spell_class"),
            "spell_level": node.get("spell_level"),
            "page_numbers": [node.get("page_start", 0)],
        })

    _log(f"  Entries built: {len(entries)}")
    return entries


def build_entries(
    markdown: str,
    heading_chapter_map: dict[int, dict],
    known_entries: set[str],
    config: dict = None,
    toc_sections: list[dict] = None,
    spell_list: list[dict] = None,
    authority_entries: list[dict] = None,
    page_texts: list[str] = None,
    page_printed: dict = None,
) -> list[dict]:
    """Build silver entries driven by ToC truth.

    Walks the ToC in order. For each section, uses entry_mode from config:
    - toc (default): one entry per ToC sub-section
    - per_list: one entry per item in reference list (spells, proficiencies)
    - per_anchor: one entry per config anchor

    Content is extracted from markdown by finding headings/anchors that match
    entry names, then taking content between consecutive matches.
    """
    from rapidfuzz import fuzz

    if not toc_sections:
        return []

    section_cfg = config.get("section_parsing", {}) if config else {}
    level_mapping = config.get("spell_level_mapping", {}) if config else {}
    type_mapping = config.get("entry_type_mapping", {}) if config else {}
    min_content = config.get("ingestion", {}).get("min_entry_content", 10) if config else 10

    # Build spell list lookup: name -> {spell_class, spell_level, school, sphere, ...}
    spell_lookup = {}
    if spell_list:
        for s in spell_list:
            spell_lookup[s.get("spell_name", "").lower()] = s

    # Build authority entries lookup: name -> {entry_type, ...}
    authority_lookup = {}
    if authority_entries:
        for a in authority_entries:
            authority_lookup[a.get("entry_name", "").lower()] = a

    # ── Index all headings in markdown by position ──
    lines = markdown.split("\n")
    line_starts = []
    pos = 0
    for line in lines:
        line_starts.append(pos)
        pos += len(line) + 1

    # heading_positions: [(line_idx, clean_title, raw_line), ...]
    heading_positions = []
    for i, line in enumerate(lines):
        m = re.match(r"^(#{1,4})\s+(.+)", line)
        if m:
            raw_heading = m.group(2).strip()
            clean = re.sub(r"\*+", "", raw_heading).strip()
            clean = re.sub(r"\s*\([\w/,\s]+\)\s*$", "", clean).strip()
            heading_positions.append((i, clean, line))

    # Also find bold-text anchors: **Title** at start of line
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("**") and "**" in stripped[2:]:
            end = stripped.index("**", 2)
            title = stripped[2:end].strip()
            if len(title) >= 3:
                heading_positions.append((i, title, line))

    heading_positions.sort(key=lambda x: x[0])

    def _find_heading(name: str, after_line: int = 0, before_line: int = None) -> int:
        """Find the first heading matching name after after_line. Returns line index or -1."""
        if before_line is None:
            before_line = len(lines)
        name_lower = name.lower()
        for line_idx, clean_title, raw in heading_positions:
            if line_idx < after_line or line_idx >= before_line:
                continue
            if clean_title.lower() == name_lower:
                return line_idx
            if fuzz.ratio(clean_title.lower(), name_lower) >= 85:
                return line_idx
        return -1

    def _extract_content(start_line: int, end_line: int) -> str:
        """Extract and clean content between two line indices."""
        raw = "\n".join(lines[start_line:end_line]).strip()
        return _clean_entry_content(raw, config) if config else raw

    def _toc_spell_class(toc_entry: dict) -> str:
        """Determine spell class from ToC entry title."""
        title = toc_entry.get("title", "").lower()
        if "wizard" in title:
            return "wizard"
        if "priest" in title:
            return "priest"
        return ""

    # ── Build chapter ranges using page positions (not heading matching) ──
    # Page anchors map markdown char positions to printed page numbers.
    # Each chapter's content starts at its ToC page_start position in markdown.
    page_anchors = _build_page_position_map(
        markdown, page_texts, page_printed, len(page_texts), config
    )

    def _page_to_line(printed_page: int) -> int:
        """Find the markdown line nearest to a printed page number."""
        # Find the char position for this page from anchors
        best_pos = 0
        for md_pos, pp in page_anchors:
            if pp <= printed_page:
                best_pos = md_pos
            elif pp > printed_page:
                break
        # Convert char position to line index
        for li in range(len(line_starts) - 1, -1, -1):
            if line_starts[li] <= best_pos:
                return li
        return 0

    chapters = [s for s in toc_sections if s.get("is_chapter") and not s.get("is_excluded")]
    chapter_ranges = {}  # chapter_title -> (start_line, end_line)
    for i, ch in enumerate(chapters):
        raw_start = _page_to_line(ch.get("page_start", 0))
        # Back up to nearest heading before the page anchor — page anchors
        # can land mid-page, missing content at the top of the first page
        start = raw_start
        for back in range(raw_start, max(0, raw_start - 30), -1):
            if re.match(r"^#{1,4}\s", lines[back]):
                start = back
                break
        # End = line position of next chapter's page, or end of doc
        if i + 1 < len(chapters):
            end = _page_to_line(chapters[i + 1].get("page_start", 9999))
        else:
            end = len(lines)
        chapter_ranges[ch["title"]] = (start, end)

    # ── Walk ToC, build entries ──
    entries = []

    for ch in chapters:
        ch_range = chapter_ranges.get(ch["title"])
        if not ch_range:
            continue
        ch_start, ch_end = ch_range

        # Determine entry_mode for this chapter
        entry_mode = "toc"  # default
        matched_cfg = None
        for sec_key, sec_cfg in section_cfg.items():
            if sec_key.lower() in ch["title"].lower():
                entry_mode = sec_cfg.get("entry_mode", "toc")
                matched_cfg = sec_cfg
                break

        # Get sub-sections for this chapter from ToC
        sub_sections = [s for s in toc_sections
                        if s.get("parent_title") == ch["title"]
                        and not s.get("is_excluded")
                        and not s.get("is_table")]

        if entry_mode == "toc":
            # One entry per ToC sub-section + chapter intro
            if sub_sections:
                # Chapter intro: content before first sub-section
                first_sub_line = -1
                for sub in sub_sections:
                    pos = _find_heading(sub["title"], ch_start, ch_end)
                    if pos > 0:
                        first_sub_line = pos
                        break
                if first_sub_line > ch_start + 1:
                    content = _extract_content(ch_start, first_sub_line)
                    if len(content) > min_content:
                        entries.append({
                            "toc_entry": ch,
                            "section_title": ch["title"].split(":", 1)[-1].strip(),
                            "entry_title": ch["title"].split(":", 1)[-1].strip(),
                            "content": content,
                            "school": None, "sphere": None,
                            "spell_class": None, "spell_level": None,
                            "page_numbers": [ch.get("page_start", 0)],
                        })

                # Each sub-section
                for idx, sub in enumerate(sub_sections):
                    sub_start = _find_heading(sub["title"], ch_start, ch_end)
                    if sub_start < 0:
                        continue
                    # End = next sub-section or chapter end
                    sub_end = ch_end
                    for nxt in sub_sections[idx + 1:]:
                        nxt_pos = _find_heading(nxt["title"], sub_start + 1, ch_end)
                        if nxt_pos > 0:
                            sub_end = nxt_pos
                            break
                    content = _extract_content(sub_start, sub_end)
                    if len(content) > min_content:
                        entries.append({
                            "toc_entry": ch,
                            "section_title": sub["title"],
                            "entry_title": sub["title"],
                            "content": content,
                            "school": None, "sphere": None,
                            "spell_class": None, "spell_level": None,
                            "page_numbers": [sub.get("page_start", 0)],
                        })
            else:
                # No sub-sections — whole chapter is one entry
                content = _extract_content(ch_start, ch_end)
                if len(content) > min_content:
                    entries.append({
                        "toc_entry": ch,
                        "section_title": ch["title"].split(":", 1)[-1].strip(),
                        "entry_title": None,
                        "content": content,
                        "school": None, "sphere": None,
                        "spell_class": None, "spell_level": None,
                        "page_numbers": [ch.get("page_start", 0)],
                    })

        elif entry_mode == "per_list":
            # One entry per item in reference list (spells, proficiencies)
            list_source = matched_cfg.get("list_source", "") if matched_cfg else ""
            list_filter = matched_cfg.get("list_filter_type", "") if matched_cfg else ""
            spell_class = _toc_spell_class(ch)

            if list_source == "spell_list_entries":
                names = [s.get("spell_name", "") for s in (spell_list or [])
                         if not spell_class or s.get("spell_class", "").lower() == spell_class]
            elif list_source == "authority_table_entries":
                names = [a.get("entry_name", "") for a in (authority_entries or [])
                         if not list_filter or a.get("entry_type", "") == list_filter]
            else:
                names = []

            # Find each entry name as a heading within the chapter
            found = []
            for name in names:
                pos = _find_heading(name, ch_start, ch_end)
                if pos >= 0:
                    found.append((pos, name))
            found.sort(key=lambda x: x[0])

            # Extract content between consecutive entries
            current_spell_level = None
            sub_pat = matched_cfg.get("sub_section_pattern", "") if matched_cfg else ""
            for idx, (pos, name) in enumerate(found):
                # Check for sub-section heading (spell level) between entries
                if sub_pat:
                    for hp_line, hp_clean, hp_raw in heading_positions:
                        if hp_line >= (found[idx - 1][0] if idx > 0 else ch_start) and hp_line < pos:
                            if re.match(sub_pat, hp_clean, re.IGNORECASE):
                                for word, level in level_mapping.items():
                                    if word in hp_clean.lower():
                                        current_spell_level = level
                                        break

                end_pos = found[idx + 1][0] if idx + 1 < len(found) else ch_end
                content = _extract_content(pos, end_pos)
                if len(content) <= min_content:
                    continue

                # Extract spell metadata
                school = _extract_school_from_raw(content) if config else None
                spell_info = spell_lookup.get(name.lower(), {})

                entries.append({
                    "toc_entry": ch,
                    "section_title": name,
                    "entry_title": name,
                    "content": content,
                    "school": school,
                    "sphere": spell_info.get("sphere") or _extract_field_from_raw(content, "Sphere") if config else None,
                    "spell_class": spell_class or spell_info.get("spell_class"),
                    "spell_level": current_spell_level or spell_info.get("spell_level"),
                    "page_numbers": [],
                })

        elif entry_mode == "per_anchor":
            # One entry per config anchor
            anchors = matched_cfg.get("entry_anchors", []) if matched_cfg else []
            name_map = matched_cfg.get("anchor_name_map", {}) if matched_cfg else {}

            found = []
            for anchor in anchors:
                for i, line in enumerate(lines[ch_start:ch_end], ch_start):
                    if line.strip().startswith(anchor):
                        display_name = name_map.get(anchor, anchor.replace("**", "").strip())
                        found.append((i, display_name))
                        break
            found.sort(key=lambda x: x[0])

            for idx, (pos, name) in enumerate(found):
                end_pos = found[idx + 1][0] if idx + 1 < len(found) else ch_end
                content = _extract_content(pos, end_pos)
                if len(content) > min_content:
                    entries.append({
                        "toc_entry": ch,
                        "section_title": name,
                        "entry_title": name,
                        "content": content,
                        "school": None, "sphere": None,
                        "spell_class": None, "spell_level": None,
                        "page_numbers": [],
                    })

    return entries


def _extract_school_from_raw(raw_content: str) -> str:
    """Extract spell school from raw content."""
    known_schools = {
        "abjuration", "alteration", "conjuration", "conjuration/summoning",
        "divination", "enchantment", "enchantment/charm", "evocation",
        "illusion", "illusion/phantasm", "invocation", "invocation/evocation",
        "necromancy", "universal", "all schools",
    }
    for line in raw_content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#"):
            paren_start = stripped.rfind("(")
            paren_end = stripped.rfind(")")
            if paren_start > 0 and paren_end > paren_start:
                school = stripped[paren_start + 1:paren_end].replace("*", "").strip()
                parts = [p.strip().lower() for p in school.replace("/", ",").split(",")]
                if any(p in known_schools for p in parts):
                    return school
            continue
        clean = stripped.replace("*", "").strip()
        if clean.startswith("("):
            inner_end = clean.find(")")
            if inner_end > 0:
                inner = clean[1:inner_end].strip()
                parts = [p.strip().lower() for p in inner.replace("/", ",").split(",")]
                if any(p in known_schools for p in parts):
                    return inner
    for line in raw_content.split("\n"):
        stripped = line.strip()
        if stripped.lower().startswith("school:"):
            return stripped[7:].strip() or None
    return None


def _extract_field_from_raw(raw_content: str, field_name: str) -> str:
    """Extract a metadata field value from raw content."""
    field_lower = field_name.lower() + ":"
    for line in raw_content.split("\n"):
        line_lower = line.strip().lower()
        if field_lower in line_lower:
            idx = line_lower.index(field_lower)
            value = line.strip()[idx + len(field_name) + 1:].strip()
            return value if value else None
    return None


def _build_entries_legacy(
    markdown: str,
    heading_chapter_map: dict[int, dict],
    known_entries: set[str],
    config: dict = None,
    toc_sections: list[dict] = None,
) -> list[dict]:
    """LEGACY: Parse Marker markdown into entries using heading-chapter map for ToC assignment.

    Kept for reference. Use build_entries() (ToC-driven) instead."""
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
            # Case-insensitive to catch all variants
            for sec in _toc_sections:
                if not sec.get("is_chapter"):
                    continue
                sec_title = sec.get("title", "")
                # Strip "(Chapter N)" using the part before ":"
                ch_prefix = sec_title.split(":")[0].strip()
                if ch_prefix:
                    content = _case_insensitive_replace(content, f"({ch_prefix})", "")
                # Strip full "(Chapter N: Title)"
                content = _case_insensitive_replace(content, f"({sec_title})", "")

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
    # Track which headings have been used as entry boundaries per chapter.
    # First occurrence = real boundary, duplicates = content (Marker page headers).
    seen_headings_per_chapter = set()  # (chapter_title, heading_name_lower)

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
                    match_lower = match_name.lower()
                    for sec in _toc_sections:
                        if not sec.get("is_chapter"):
                            continue
                        sec_title = sec.get("title", "")
                        sec_desc = sec_title.split(":", 1)[-1].strip().lower()
                        if match_lower == sec_title.lower() or match_lower == sec_desc:
                            is_chapter_ref = True
                            break

                    if is_chapter_ref:
                        current_content.append(line)
                    else:
                        # Check if this heading was already used as a boundary in this chapter
                        chapter_key = current_toc.get("title", "") if current_toc else ""
                        heading_key = (chapter_key, match_name.lower())
                        if heading_key in seen_headings_per_chapter:
                            # Duplicate heading — discard (Marker page header repeat)
                            pass
                        else:
                            # In whitelist sections, only create entries for known headings
                            # In non-whitelist sections, every H3/H4 heading creates a new entry
                            in_whitelist = _is_whitelist_section(current_toc, config)
                            if in_whitelist and known_entries and match_name.lower() not in known_entries:
                                current_content.append(line)
                            else:
                                seen_headings_per_chapter.add(heading_key)
                                flush()
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
