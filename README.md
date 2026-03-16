# lakehouse
An open source python centric sufficient lake house software stack with some AI support

## Quick Start - PDF Projects

Create a new PDF data project with ingestion + RAG search in seconds:

```bash
python notebooks/setup_pdf_project.py
```

This interactive wizard will:
1. Ask for project name, document type, use case, and metadata fields
2. Auto-generate all necessary files
3. Create isolated DuckDB schema and ChromaDB collection

**Result:** A complete project with PDF ingestion, semantic search, and LLM-powered Q&A

### Example Projects Already Set Up

- **tabletop_rules** - RPG/board game rules (D&D, Pathfinder, etc.)
  - Location: `documents/tabletop_rules/`
  - Files: `dlt/load_tabletop_rules_docs.py`, `rag/embed_tabletop_rules.py`, `rag/query_tabletop_rules.py`

### Full Documentation

- **[Python-First Lakehouse Architecture](./ai/python_lakehouse_architecture.md)** - High-level design for small data teams
- **[PDF_PROJECT_SETUP.md](./PDF_PROJECT_SETUP.md)** - Complete setup guide with examples
- **[QUICK_START_PROJECTS.md](./QUICK_START_PROJECTS.md)** - Cheat sheet for creating projects
