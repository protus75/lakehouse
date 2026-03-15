## Quick Reference - PDF Project Setup

### To Create a New Project

**Interactive method (recommended):**
```bash
python notebooks/setup_pdf_project.py
```

**Programmatic method:**
```python
from tools.pdf_project_generator import create_project

create_project(
    project_name="My Project Name",
    project_slug="my_project_name",
    doc_type="Research papers", # or Business documents, Technical docs, Other/Mixed
    use_case="Both (Recommended)",  # or Q&A / Search, Analysis / Reporting
    metadata_fields=["field1", "field2", "field3"]
)
```

### Generated Project Workflow

For any created project (example: `tabletop_rules`):

**1. Add PDFs**
```bash
cp *.pdf documents/tabletop_rules/raw/
```

**2. Ingest** (Jupyter)
```python
from dlt.load_tabletop_rules_docs import run
run(game_system="D&D 5e", content_type="rules")
```

**3. Embed**
```python
from rag.embed_tabletop_rules import embed_all
embed_all()
```

**4. Query**
```python
from rag.query_tabletop_rules import ask
answer = ask("How do you calculate armor class?")
print(answer)
```

### What Gets Created

For project slug `<project_slug>`:

```
dlt/load_<project_slug>_docs.py       # PDF parsing & chunking
rag/embed_<project_slug>.py           # Embeddings generation
rag/query_<project_slug>.py           # Q&A with RAG
documents/<project_slug>/README.md    # Quick reference
documents/<project_slug>/raw/         # PDF storage folder
documents/<project_slug>/processed/   # Future preprocessing
```

### Existing Projects

- **tabletop_rules** - RPG/board game rules (D&D, Pathfinder, etc.)
  - Metadata: game_system, content_type, tags, rules_version
  - Already setup in: `dlt/load_tabletop_rules_docs.py`, etc.

### Key Files

- `PDF_PROJECT_SETUP.md` - Full documentation
- `tools/pdf_project_generator.py` - Core generator
- `notebooks/setup_pdf_project.py` - Interactive wizard
