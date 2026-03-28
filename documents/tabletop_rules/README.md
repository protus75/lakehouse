# Tabletop Rules Project

PDF-to-Iceberg pipeline for tabletop RPG rules. Extracts, structures, and enriches game content with AI summaries.

## Architecture

```
PDF → Bronze (dlt→Iceberg) → Silver (dbt) → Gold (dbt) → Publish (Iceberg) → AI Enrichment (Ollama)
```

All pipeline runs go through **Dagster** at http://localhost:3000.

## Current Focus

**Player's Handbook (D&D 2e)** — one book until validation passes.

## Pipeline Layers

### Bronze (`dlt/`)
- PDF extraction via pymupdf (page_texts) + Marker (headings)
- Raw content stored in Iceberg tables (`bronze_tabletop.*`)
- OCR validation via dictionary-based spellcheck

### Silver (`dbt/`)
| Model | Purpose |
|-------|---------|
| `silver_entries` | Cleaned entries with stable entry_id, toc_id |
| `silver_toc_sections` | Table of contents with section hierarchy |
| `silver_entry_descriptions` | Clean prose descriptions (metadata headers stripped) |
| `silver_spell_meta` | Structured spell metadata (school, range, duration, etc.) |
| `silver_spell_crosscheck` | Fuzzy-matched spells across appendix sources |
| `silver_known_entries` | Authority table entries from indexes |
| `silver_page_anchors` | Markdown position → PDF page mapping |

### Gold (`dbt/`)
| Model | Purpose |
|-------|---------|
| `gold_entry_index` | Structured cross-reference index for queries |
| `gold_entries` | Full entry content in reading order |
| `gold_chunks` | 800-char chunks with overlap for RAG |
| `gold_entry_descriptions` | Clean descriptions + AI summaries (type='original'/'summary') |
| `gold_toc` | Filtered table of contents |

### AI Enrichment (`scripts/tabletop_rules/`)
| Script | Model | Output |
|--------|-------|--------|
| `enrich_summaries.py` | `qwen3:30b-a3b` | 1-3 sentence summaries → `gold_entry_descriptions` |
| `enrich_annotations.py` | `llama3:70b` | Combat/popular flags → `gold_ai_annotations` |

Enrichment is resumable — skips already-processed entries.

## Browser

Streamlit app at http://localhost:8000 (exposed via Cloudflare Tunnel at gamerules.ai).

- Full scrollable book with ToC sidebar navigation
- AI Summary toggle: swaps spell descriptions between original prose and AI summary
- Entry metadata badges (type, level, class, school, sphere, combat, popular)
- Search across all entries

## Key Configs

- **Per-book config:** `documents/tabletop_rules/configs/DnD2e_Handbook_Player.yaml`
- **Default config:** `documents/tabletop_rules/configs/_default.yaml`
- **Lakehouse infra:** `config/lakehouse.yaml`

## Directory Structure

```
documents/tabletop_rules/
├── raw/                    ← Source PDFs
├── processed/marker/       ← Marker OCR cache
├── configs/                ← Per-book YAML configs
│   ├── _default.yaml       ← Shared defaults + enrichment settings
│   └── DnD2e_Handbook_Player.yaml
└── reviews/                ← ToC review notes
```
