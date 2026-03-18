# Prompt: Create a Book Config for the RAG Pipeline

Copy the prompt below and paste it into a conversation with Claude (or another LLM).
Attach or paste the table of contents pages and 2-3 sample content pages from your PDF.

---

## Prompt

I need you to create a YAML config file for parsing a tabletop RPG PDF into a RAG (retrieval-augmented generation) system. The config controls how the book is chunked, what structured entries are detected (spells, monsters, items, etc.), and how summaries and cross-references are generated.

Below I'll provide:
1. The table of contents (first few pages of the book)
2. Two or three sample pages showing typical structured entries (stat blocks, spells, abilities, etc.)

From this, I need you to produce a complete YAML config file.

### Config Schema

Here is the full schema with explanations. An example for the AD&D 2e Player's Handbook follows.

```yaml
# Filename must match the PDF: "My Book Name.yaml" for "My Book Name.pdf"

book:
  title: "Full Book Title"          # Used in LLM prompts
  game_system: "System Name"        # e.g. "D&D 5e", "Pathfinder 2e", "Warhammer 40K"
  content_type: "rules"             # "rules", "module", "campaign", "supplement", "bestiary"

toc:
  # Regex patterns to match chapter/section headings in the ToC
  # These extract "Title ... page_number" lines from the first N pages
  # The pattern must match the TITLE portion; the pipeline appends "\.{2,}.*(\d+)$" for page numbers
  chapter_patterns:
    - '(?:Chapter|Appendix)\s+\d*\s*:?\s*[A-Za-z].*'
  # Pattern for named tables in the ToC (set to "" if no named tables)
  table_pattern: 'Table\s+\d+\s*:\s*[A-Za-z].*'
  # How many PDF pages to scan for the ToC (front matter pages)
  toc_scan_pages: 15

# Headings that mark sections to EXCLUDE from parsing (indexes, spell lists with just names+pages)
# Case-insensitive exact match, plus anything ending in "index" is auto-excluded
index_headings:
  - "index"
  - "alphabetical index"
  # Add book-specific ones like "spell index", "monster index", etc.

# Entry types define structured content the book contains.
# Each type has detection rules and metadata extraction patterns.
# You can define multiple types (spell, monster, magic_item, feat, class_feature, etc.)
# Set to {} if the book has no structured entries.
entry_types:
  # The key name (e.g. "spell") is arbitrary but should describe the entry type
  spell:
    # An entry is this type if N+ of these regex patterns match in its content
    # These are the KEY: VALUE field labels that appear in the stat block
    # Don't include \b prefix — the code adds it
    detect_fields:
      - 'Range\s*:'
      - 'Components\s*:'
      - 'Duration\s*:'
      - 'Casting Time\s*:'
    # Minimum number of detect_fields that must match
    detect_min_fields: 3
    # Additional required fields — at least one must match (for classification)
    # Leave empty [] if not needed
    detect_class_fields:
      - 'School\s*:'
      - 'Sphere\s*:'
    # Chapter titles containing these words are strong signals for this entry type
    chapter_keywords:
      - "spell"
    # Metadata to extract from each entry via regex
    # Key = metadata field name, Value = regex with one capture group for the value
    # Don't include \b prefix — the code adds it
    metadata:
      school: 'School\s*:\s*(.+?)(?:\n|$)'
      range: 'Range\s*:\s*(.+?)(?:\n|$)'
    # Rules for determining the class/category of the entry from chapter context
    # Evaluated in order; first match wins
    class_rules:
      - chapter_contains: "priest"   # If chapter title contains this word...
        value: "Priest"              # ...assign this class
      - has_field: "sphere"          # If this metadata field was extracted...
        value: "Priest"              # ...assign this class
      - default: "Wizard"            # Fallback if no rule matches
    # Map words in section titles to level numbers
    # e.g. "First-Level Spells" → level 1
    level_patterns:
      "first": 1
      "second": 2
      "third": 3
      # ... up to the max level in this system

cross_references:
  # Parse existing appendix/index content in the book into per-category chunks
  appendix_indexes:
    - chapter_contains: "school"                              # Match chapters with this word
      label_template: "Spell Index: Wizard School - {section}" # {section} = the section heading
  # Generate index chunks from parsed entry metadata
  generated_indexes:
    - group_by: ["class", "level"]                            # Group entries by these metadata fields
      label_template: "{class} Spells - Level {level}"
      chapter_template: "Cross-Reference: {class} Spells"
    - group_by: ["alpha"]                                     # Special: alphabetical grouping
      label_template: "Spell Index: {letter}"
      chapter_template: "Cross-Reference: Alphabetical Spell Index"

# Regex patterns for detecting pages where the PDF parser may have dropped structured content
# Used to trigger VLM (vision model) re-extraction of those pages
# Include all key:value field patterns from the book's stat blocks
vlm_detection_patterns:
  - '(?:Range|Components|Duration|Casting Time)\s*:'
  - '(?:AC|Hit Dice|Movement)\s*:'

# LLM prompt templates for summary generation
# Available placeholders: {book_title}, {game_system}, {section}, {chapter}, {content}, {spells}
prompts:
  # Summary for each ToC subsection
  section_summary: |
    You are summarizing a section from the {book_title} for a rules reference.
    Write a concise 2-4 sentence summary of this section that captures:
    - What this section covers
    - The key rules or mechanics described
    - Any important numbers, thresholds, or exceptions
    SECTION: {section}
    CHAPTER: {chapter}
    CONTENT:
    {content}
    SUMMARY:
  # Summary for each structured entry (spell, monster, etc.)
  # Set to "" to skip entry summaries
  entry_summary: |
    You are creating concise structured summaries of {game_system} spells.
    For each spell below, produce a summary in this exact format:
    SPELL: [Name]
    Type: [Class] Level [N]
    Key Effect: [1 sentence]
    Important Details: [1-2 sentences]
    ---
    {spells}
    ---

chunking:
  max_chars: 800    # Max characters per chunk
  overlap: 200      # Overlap between consecutive chunks within a section
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

index_headings:
  - "index"
  - "spell index"
  - "priest spell index"
  - "wizard spell index"

entry_types:
  spell:
    detect_fields:
      - 'Range\s*:'
      - 'Components\s*:'
      - 'Duration\s*:'
      - 'Casting Time\s*:'
      - 'Area of Effect\s*:'
      - 'Saving Throw\s*:'
    detect_min_fields: 3
    detect_class_fields:
      - 'School\s*:'
      - 'Sphere\s*:'
    chapter_keywords: ["spell", "appendix"]
    metadata:
      school: 'School\s*:\s*(.+?)(?:\n|$)'
      sphere: 'Sphere\s*:\s*(.+?)(?:\n|$)'
      range: 'Range\s*:\s*(.+?)(?:\n|$)'
      components: 'Components\s*:\s*(.+?)(?:\n|$)'
      duration: 'Duration\s*:\s*(.+?)(?:\n|$)'
      casting_time: 'Casting Time\s*:\s*(.+?)(?:\n|$)'
      area_of_effect: 'Area of Effect\s*:\s*(.+?)(?:\n|$)'
      saving_throw: 'Saving Throw\s*:\s*(.+?)(?:\n|$)'
    class_rules:
      - chapter_contains: "priest"
        value: "Priest"
      - chapter_contains: "wizard"
        value: "Wizard"
      - has_field: "sphere"
        value: "Priest"
      - default: "Wizard"
    level_patterns:
      "first": 1
      "second": 2
      "third": 3
      "fourth": 4
      "fifth": 5
      "sixth": 6
      "seventh": 7
      "eighth": 8
      "ninth": 9

cross_references:
  appendix_indexes:
    - chapter_contains: "school"
      label_template: "Spell Index: Wizard School - {section}"
    - chapter_contains: "sphere"
      label_template: "Spell Index: Priest Sphere - {section}"
  generated_indexes:
    - group_by: ["class", "level"]
      label_template: "{class} Spells - Level {level}"
      chapter_template: "Cross-Reference: {class} Spells"
    - group_by: ["alpha"]
      label_template: "Spell Index: {letter}"
      chapter_template: "Cross-Reference: Alphabetical Spell Index"

vlm_detection_patterns:
  - '(?:Range|Components|Duration|Casting Time|Area of Effect|Saving Throw)\s*:'
  - '(?:Power Score|PSP Cost|Initial Cost|Maintenance Cost)\s*:'
  - '(?:AC|THAC0|Hit Dice|No\. of Attacks|Damage/Attack|Movement)\s*:'

prompts:
  section_summary: |
    You are summarizing a section from the {book_title} for a rules reference.
    Write a concise 2-4 sentence summary of this section that captures:
    - What this section covers
    - The key rules or mechanics described
    - Any important numbers, thresholds, or exceptions
    SECTION: {section}
    CHAPTER: {chapter}
    CONTENT:
    {content}
    SUMMARY:
  entry_summary: |
    You are creating concise structured summaries of {game_system} spells for a rules reference.
    For each spell below, produce a summary in this exact format:
    SPELL: [Name]
    Type: [Wizard/Priest] Level [N]
    School: [school] | Sphere: [sphere if priest]
    Key Effect: [1 sentence describing what the spell does]
    Important Details: [1-2 sentences on range, duration, damage, saving throw effects]
    Tactical Note: [1 sentence on when/why to use this spell]
    ---
    {spells}
    ---

chunking:
  max_chars: 800
  overlap: 200
```

### Your Task

Using the table of contents and sample pages I provide below, create a complete YAML config file for my book. Specifically:

1. **book** — fill in the title, game system, and content type
2. **toc** — look at the ToC format and write regex patterns that match the chapter/section headings. Note whether chapters use "Chapter N:", "Part N:", roman numerals, or other formats
3. **index_headings** — identify any index or reference sections at the end that should be excluded
4. **entry_types** — look at the sample pages for repeating structured entries (stat blocks). Identify:
   - What fields appear in each stat block (the "Key: Value" lines)
   - Which fields distinguish entry categories (e.g. School vs Sphere for D&D spell types)
   - How entries are organized by chapter (which chapter keywords signal this entry type)
   - What level/tier system exists and how it appears in section titles
5. **cross_references** — identify any existing index appendices in the ToC, and what groupings would be useful
6. **vlm_detection_patterns** — combine all the stat block field patterns
7. **prompts** — adapt the summary prompts to use terminology appropriate for this game system
8. **chunking** — 800/200 is good for most books, but if entries are very short or very long, adjust

Output ONLY the YAML config file with comments explaining your choices.

### My Book

**Table of Contents:**
[PASTE TOC PAGES HERE]

**Sample Content Pages:**
[PASTE 2-3 PAGES SHOWING TYPICAL STRUCTURED ENTRIES HERE]
