# Prompt: Create a Book Config for the RAG Pipeline

Copy the prompt below and paste it into a conversation with Claude (or another LLM).
Attach or paste the table of contents pages and 2-3 sample content pages from your PDF.

---

## Prompt

I need you to create a YAML config file for parsing a tabletop RPG PDF into a RAG system.

The parsing pipeline works as follows:
1. **pymupdf** reads printed page numbers from each PDF page and maps them to ToC chapters
2. **Marker** (ML-based PDF parser) extracts the full document as clean markdown with proper headings, tables, and column handling
3. Each Marker heading gets assigned its chapter by finding that heading text in the pymupdf page text (forward search)
4. Known entry names from excluded index sections act as a whitelist — only headings matching known entries create new entries in spell/ability sections
5. Config-driven `strip_content_patterns` remove school/type annotations and tags from entry content

The config only needs to define:
- How to find chapters in the ToC
- What to exclude (indexes, reference lists)
- What lines to strip from content (school annotations, tags)
- Validation settings

**IMPORTANT:** The config should NOT contain regex for heading detection, metadata extraction, or content parsing. Marker handles all of that with ML-based layout analysis. The config only handles ToC structure, exclusions, and content cleanup.

### Config Schema

```yaml
# Filename must match the PDF: "My Book Name.yaml" for "My Book Name.pdf"

book:
  title: "Full Book Title"          # Used in LLM prompts
  game_system: "System Name"        # e.g. "D&D 5e", "Pathfinder 2e"
  content_type: "rules"             # "rules", "module", "campaign", "supplement", "bestiary"

toc:
  # Regex to match chapter/section headings in the ToC pages
  # Only needed for the ToC page format, not content parsing
  # The pipeline appends dot-leader + page number matching automatically
  chapter_patterns:
    - '(?:Chapter|Appendix)\s+\d*\s*:?\s*[A-Za-z].*'
  # Pattern for named tables in the ToC (set to "" if none)
  table_pattern: 'Table\s+\d+\s*:\s*[A-Za-z].*'
  # How many PDF pages to scan for the ToC
  toc_scan_pages: 15
  # Regex to find the printed page number on each page
  # Look at sample pages to see format — usually a standalone number at top or bottom
  page_number_pattern: '^\d{1,3}$'

# Headings within pages that mark an index section to stop processing
index_headings:
  - "index"

# Lines matching these regex patterns are stripped from entry content during ingestion
# Used to remove school/type annotations that belong in metadata, not description
# e.g. "(Conjuration/Summoning)" or "Reversible" in D&D spell sections
strip_content_patterns: []

# ToC entries to exclude entirely — indexes, reference lists, compiled tables
# CRITICAL: list every ToC entry that contains entry NAMES without actual content
# If not excluded, the parser will match entry names in these pages
# and assign wrong chapters
exclude_chapters: []

# Validation settings for the validate_spells.py script
validation:
  # ToC title substrings to match spell/entry sections
  spell_toc_patterns:
    - "spell"
  # Metadata fields that should be present in every entry
  required_metadata:
    - "Range:"
    - "Duration:"
    - "Casting Time:"

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

strip_content_patterns:
  - '^\([\w/,\s*]+\)\s*(?:Reversible)?\s*$'
  - '^Reversible\s*$'
  - '^\([\w/,\s*]+\)\s+Reversible\s*$'

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
    - "Range:"
    - "Components:"
    - "Duration:"
    - "Casting Time:"
    - "Area of Effect:"
    - "Saving Throw:"

chunking:
  max_chars: 800
  overlap: 200
```

### Your Task

Using the table of contents and sample pages I provide below, create a complete YAML config file. Specifically:

1. **book** — fill in title, game system, and content type
2. **toc** — look at the ToC format and write patterns that match chapter/section headings. Check sample pages for how page numbers are printed and write the `page_number_pattern`
3. **index_headings** — identify heading text within pages that marks an index section
4. **exclude_chapters** — CRITICAL: list every ToC entry that is a reference list, index, compiled table, or cross-reference rather than actual content. Look for: spell lists, monster indexes, equipment tables, compiled charts. If not excluded, entry names in these pages will cause incorrect chapter assignment.
5. **strip_content_patterns** — look at the sample content pages. If entries have school/type annotations in parentheses (like "(Conjuration/Summoning)") or tags (like "Reversible") that appear as standalone lines, write regex patterns to strip them. These are metadata, not description content.
6. **validation** — set the ToC title substrings that match entry sections, and list the metadata fields every entry should have
7. **chunking** — 800/200 is good for most books

Output ONLY the YAML config file with comments explaining your choices.

### My Book

**Table of Contents:**
[PASTE TOC PAGES HERE]

**Sample Content Pages:**
[PASTE 2-3 PAGES SHOWING TYPICAL STRUCTURED ENTRIES HERE]
