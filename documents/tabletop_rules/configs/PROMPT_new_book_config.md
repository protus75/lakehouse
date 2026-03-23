# Prompt: Create a Book Config for the RAG Pipeline

Copy the prompt below and paste it into a conversation with Claude (or another LLM).
Attach or paste the table of contents pages and 2-3 sample content pages from your PDF.

---

## Prompt

I need you to create a YAML config file for parsing a tabletop RPG PDF into a RAG system.

The parsing pipeline works as follows:
1. **pymupdf** caches all page text and extracts printed page numbers
2. **Marker** (ML-based PDF parser) extracts the full document as continuous markdown
3. **Page-position mapping** finds unique text snippets from each PDF page in the Marker markdown, building anchor points that locate page boundaries in the continuous markdown
4. Each heading's chapter is determined by interpolating between page anchors, then looking up which ToC section that page belongs to — no text matching against chapter titles
5. Known entry names from excluded index sections act as a whitelist — only headings matching known entries create new entries in spell/ability sections
6. Content cleanup uses string operations (not regex) to split smashed metadata fields and fix OCR artifacts via config-driven substitutions
7. Validation checks spells (metadata fields), sections (chunk distribution), and content (expected entries and tables per section)

### What the config controls

| Section | Purpose |
|---------|---------|
| `book` | Metadata: title, game system, content type |
| `toc` | How to find chapters in the ToC pages (pattern for chapter lines, page number format) |
| `exclude_chapters` | ToC entries to skip (indexes, reference lists, compiled tables) |
| `index_headings` | Heading text that marks index sections (for entry name whitelist extraction) |
| `metadata_field_names` | Field names for splitting smashed metadata (e.g. "Range", "Duration") |
| `strip_content_patterns` | Lines to remove from entry content (school annotations, tags) |
| `content_substitutions` | OCR artifact fixes as [find, replace] pairs |
| `validation` | Spell metadata requirements, section content checks |
| `ingestion` | Thresholds for cleanup, dedup, index extraction |
| `chunking` | Chunk size and overlap |

**IMPORTANT:** The config should NOT contain regex for heading detection, metadata extraction, or content classification. Marker handles layout analysis with ML. Use string patterns only for ToC line matching and content strip patterns that the user defines.

### Config Schema

```yaml
# Filename must match the PDF: "My Book Name.yaml" for "My Book Name.pdf"

book:
  title: "Full Book Title"          # Used in LLM prompts and metadata
  game_system: "System Name"        # e.g. "D&D 2e", "Pathfinder 2e"
  content_type: "rules"             # "rules", "module", "campaign", "supplement", "bestiary"

toc:
  # Pattern to match chapter/section headings in the ToC pages
  # The pipeline uses _extract_toc_line() to find title + page number pairs,
  # then matches the title against these patterns
  chapter_patterns:
    - '(?:Chapter|Appendix)\s+\d*\s*:?\s*[A-Za-z].*'
  # Pattern for named tables in the ToC (set to "" if none)
  table_pattern: 'Table\s+\d+\s*:\s*[A-Za-z].*'
  # How many PDF pages to scan for the ToC
  toc_scan_pages: 15
  # Regex to find the printed page number on each page
  # Look at sample pages — usually a standalone number at top or bottom
  page_number_pattern: '^\d{1,3}$'

# Headings within pages that mark index sections for entry name extraction
index_headings:
  - "index"

# Lines matching these patterns are stripped from entry content during ingestion
# Used for school/type annotations that belong in metadata, not description
# e.g. "(Conjuration/Summoning)" or "Reversible" in D&D spell sections
strip_content_patterns: []

# OCR artifact fixes applied during content cleanup
# Each item is [find_string, replace_string]
content_substitutions:
  - ['D- M', 'DM']

# Metadata field names — used for splitting smashed fields onto separate lines
# e.g. "Sphere: All Range: 60 yds." → split into two lines
metadata_field_names:
  - Range
  - Components
  - Duration
  - Casting Time
  - Area of Effect
  - Saving Throw

# ToC entries to exclude entirely — indexes, reference lists, compiled tables
# CRITICAL: list every ToC entry that contains entry NAMES without content.
# These sections are used to build the known_entries whitelist, not for content.
exclude_chapters: []

# Validation settings
validation:
  # ToC title substrings to match spell/entry sections for spell validation
  spell_toc_patterns:
    - "spell"
  # Metadata fields every spell entry should have
  required_metadata:
    - "Range"
    - "Duration"
    - "Casting Time"
  # Regex patterns for legitimate hyphenated words to exclude from validation
  hyphen_exclude_patterns:
    - '\d+(?:st|nd|rd|th)-'

  # Expected content per section — validator checks these appear in chunk content
  # Keys are substrings matched against ToC section titles
  section_content:
    "Section Name":
      expected_entries:
        - "Entry Name 1"
        - "Entry Name 2"
      expected_tables:
        - "Table 1"
        - "Table 2"

# Ingestion thresholds (override defaults from _default.yaml if needed)
# ingestion:
#   min_entry_content: 10
#   max_fragment_length: 60
#   max_interblock_fragment_length: 80
#   min_description_block: 15
#   dedup_signature_chars: 80
#   max_smashed_metadata_value: 40
#   max_sub_headings_per_section: 50
#   min_index_entry_length: 3
#   max_index_entry_length: 50

chunking:
  max_chars: 800    # Max characters per chunk
  overlap: 200      # Overlap between chunks within an entry
```

### Example: AD&D 2e Player's Handbook Config

```yaml
book:
  title: "AD&D 2nd Edition Player's Handbook"
  game_system: "D&D 2e"
  content_type: "rules"

toc:
  chapter_patterns:
    - '(?:Chapter|Appendix)\s+\d*\s*:?\s*[A-Za-z].*'
  table_pattern: 'Table\s+\d+\s*:\s*[A-Za-z].*'
  toc_scan_pages: 15
  page_number_pattern: '^\d{1,3}$'

index_headings:
  - "index"
  - "spell index"
  - "priest spell index"
  - "wizard spell index"
  - "spell list"

strip_content_patterns:
  - '^\([\w/,\s*]+\)\s*(?:Reversible)?\s*$'
  - '^Reversible\s*$'

content_substitutions:
  - ['D- M', 'DM']

exclude_chapters:
  - "Appendix 1: Spell Lists"
  - "Appendix 5: Wizard Spells by School"
  - "Appendix 6: Priest Spells by Sphere"
  - "Appendix 7: Spell Index"
  - "Appendix 8: Compiled Character Generation Tables"

validation:
  spell_toc_patterns:
    - "wizard spell"
    - "priest spell"
  required_metadata:
    - "Range"
    - "Component"
    - "Duration"
    - "Casting Time"
    - "Area of Effect"
    - "Saving Throw"
  hyphen_exclude_patterns:
    - '\d+(?:st|nd|rd|th)-'
    - 'warm-'

  section_content:
    "Player Character Classes":
      expected_entries:
        - "Fighter"
        - "Paladin"
        - "Ranger"
        - "Cleric"
        - "Druid"
        - "Mage"
        - "Thief"
        - "Bard"
      expected_tables:
        - "Table 13"
        - "Table 14"
        - "Table 15"

    "Proficiencies":
      expected_entries:
        - "Blind-fighting"
        - "Healing"
        - "Herbalism"
        - "Tracking"
        - "Swimming"
      expected_tables:
        - "Table 34"
        - "Table 37"

    "Money and Equipment":
      expected_entries:
        - "Chain Mail"
        - "Plate Mail"
        - "Long Sword"
        - "Dagger"
        - "Crossbow"
      expected_tables:
        - "Table 42"
        - "Table 43"
        - "Table 46"

    "Player Character Races":
      expected_entries:
        - "Dwarf"
        - "Elf"
        - "Gnome"
        - "Halfling"
        - "Human"
      expected_tables:
        - "Table 10"
        - "Table 12"

    "Combat":
      expected_entries:
        - "Initiative"
        - "Surprise"
        - "Saving Throw"
      expected_tables:
        - "Table 51"
        - "Table 54"

chunking:
  max_chars: 800
  overlap: 200
```

### Your Task

Using the table of contents and sample pages I provide below, create a complete YAML config file. Specifically:

1. **book** — fill in title, game system, and content type
2. **toc** — look at the ToC format and write `chapter_patterns` that match the chapter/section heading style. Check sample pages for how page numbers are printed and write `page_number_pattern`
3. **index_headings** — identify heading text within pages that marks index sections (used to build the known_entries whitelist)
4. **exclude_chapters** — CRITICAL: list every ToC entry that is a reference list, index, compiled table, or cross-reference rather than actual content. If not excluded, entry names in these pages will pollute the known_entries whitelist
5. **metadata_field_names** — list all metadata field names used in structured entries (stat blocks, spell headers, etc.)
6. **strip_content_patterns** — if entries have school/type annotations as standalone lines (like "(Conjuration/Summoning)"), write patterns to strip them
7. **content_substitutions** — if you see common OCR artifacts in sample pages, add find/replace pairs
8. **validation** — set `spell_toc_patterns` to match entry sections, `required_metadata` for mandatory fields, and `section_content` with expected entries and tables for key sections (at least 5-10 representative entries per section for spot-checking)
9. **chunking** — 800/200 is good for most books. Increase max_chars for books with very long entries

Output ONLY the YAML config file with comments explaining your choices.

### My Book

**Table of Contents:**
[PASTE TOC PAGES HERE]

**Sample Content Pages:**
[PASTE 2-3 PAGES SHOWING TYPICAL STRUCTURED ENTRIES HERE — spells, proficiencies, equipment, etc.]
