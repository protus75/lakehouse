---
name: Gold layer enrichment requirements
description: AI summaries, cross-reference indices, and AI annotations needed in gold layer
type: project
---

## Gold Layer Enrichments (requested 2026-03-23)

### 1. AI Summaries
- Spells, proficiencies, character classes, general rules
- For large text entries — concise summary for quick reference
- LLM-generated from silver_entries content

### 2. Cross-Reference Indices
- Fast DB lookups by category, not just full-text search
- Examples: first level spells, necromancy spells, cleric spells, range weapons, wizard proficiencies
- Structured tags/categories extracted from entry metadata
- Enables: "show me all 3rd level wizard evocation spells" as a DB query

### 3. AI Annotations
- **Combat**: yes/no — is the main purpose to aid in an encounter/combat?
- **Popular**: yes/no — commonly found in normal player character spell/proficiency lists
- Applied to spells and proficiencies
- LLM-classified from entry content

**Why:** Makes the RAG system useful beyond just "search for text" — enables structured browsing, filtering, and categorized retrieval.
**How to apply:** New gold dbt models reading from silver_entries, using Ollama LLM for classification.
