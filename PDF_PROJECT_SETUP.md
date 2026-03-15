# PDF Project Generator Setup

This is a reusable system for creating new PDF data projects in the lakehouse. Instead of manually creating files for each project, use the **PDF Project Setup Wizard** to automatically generate everything.

## How It Works

The system has three components:

### 1. `tools/pdf_project_generator.py`
Core generator with these functions:
- `generate_ingest_pipeline()` - Creates dlt ingestion module
- `generate_embed_module()` - Creates ChromaDB embedding module
- `generate_query_module()` - Creates RAG query module
- `generate_readme()` - Creates project README.md
- `create_project()` - Orchestrates all file generation

### 2. `notebooks/setup_pdf_project.py`
Interactive wizard with two modes:

**Mode 1: Interactive CLI**
```bash
python notebooks/setup_pdf_project.py
```
Prompts you for:
- Project name
- Document type (research papers, business, technical docs, etc.)
- Use case (Q&A, Analytics, Both)
- Metadata fields to track

**Mode 2: Jupyter notebook**
```python
# In Jupyter, can use this for interactive setup
exec(open('notebooks/setup_pdf_project.py').read())
```

### 3. `tools/pdf_project_generator.py` (Python module)
Can also be imported and called directly:

```python
from tools.pdf_project_generator import create_project

create_project(
    project_name="My Research Hub",
    project_slug="research_hub",
    doc_type="Research papers",
    use_case="Both (Recommended)",
    metadata_fields=["subject", "author", "publication_year"]
)
```

## Generated Files

For each new project, the wizard creates:

```
dlt/
└── load_<project_slug>_docs.py          # PDF ingestion pipeline

rag/
├── embed_<project_slug>.py              # ChromaDB embedding
├── query_<project_slug>.py              # RAG query + LLM
└── setup_<project_slug>.py (optional)   # Setup guide

documents/
└── <project_slug>/
    ├── raw/                              # Place PDFs here
    ├── processed/                        # For preprocessing
    └── README.md                         # Quick reference

lakehouse.duckdb
└── documents_<project_slug>/
    ├── chunks                            # Text chunks with metadata
    └── files                             # File-level metadata

chroma_db/
└── <project_slug>_chunks                # Embeddings collection
```

## Example: Creating a New Project

### Interactive CLI Method

```bash
$ python notebooks/setup_pdf_project.py

============================================================
PDF DATA PROJECT SETUP WIZARD
============================================================

Step 1 of 4: Project Information
----------------------------------------
Project name (e.g., 'Research Papers', 'Legal Documents'): Legal Case Files
✓ Project slug: legal_case_files

Step 2 of 4: Document Type
----------------------------------------
What types of documents will you be ingesting?
1. Research papers
2. Business documents (reports, contracts, policies)
3. Technical documentation (guides, API docs)
4. Other/Mixed
Select (1-4): 2
✓ Document type: Business documents

Step 3 of 4: Primary Use Case
----------------------------------------
What is the primary use case?
1. Q&A / Search only
2. Analysis / Reporting only
3. Both (Q&A + Analytics)
Select (1-3): 3
✓ Use case: Both (Recommended)

Step 4 of 4: Metadata Fields
----------------------------------------
Which metadata should be tracked?
1. Basic only (filename, document title)
2. Enhanced (filename, title, type, tags, version)
3. Custom (provide comma-separated field names)
Select (1-3): 3
Enter custom fields (comma-separated, e.g., 'category, author, date'): case_number, jurisdiction, filing_date, case_type

✓ Metadata fields: case_number, jurisdiction, filing_date, case_type

============================================================
PROJECT SUMMARY
============================================================
Name:              Legal Case Files
Slug:              legal_case_files
Document Type:     Business documents
Use Case:          Both (Recommended)
Metadata Fields:   case_number, jurisdiction, filing_date, case_type
============================================================

Create project? (y/n): y

✓ Created /workspace/dlt/load_legal_case_files_docs.py
✓ Created /workspace/rag/embed_legal_case_files.py
✓ Created /workspace/rag/query_legal_case_files.py
✓ Created /workspace/documents/legal_case_files/README.md

✅ Project 'Legal Case Files' created successfully!

Next steps:
1. Add PDFs to: documents/legal_case_files/raw/
2. Run ingestion: from dlt.load_legal_case_files_docs import run; run()
3. Run embedding: from rag.embed_legal_case_files import embed_all; embed_all()
4. Ask questions: from rag.query_legal_case_files import ask; ask('your question')
```

### Python API Method

```python
from tools.pdf_project_generator import create_project

create_project(
    project_name="Medical Research Papers",
    project_slug="medical_papers",
    doc_type="Research papers",
    use_case="Both (Recommended)",
    metadata_fields=["specialty", "publication_date", "study_type"]
)
```

## Using the Generated Project

Once created, all projects follow the same workflow:

### 1. Add PDFs
```bash
cp my_documents/*.pdf documents/medical_papers/raw/
```

### 2. Ingest & Parse
```python
from dlt.load_medical_papers_docs import run
run(specialty="Cardiology", study_type="clinical_trial")
```

### 3. Embed for Semantic Search
```python
from rag.embed_medical_papers import embed_all
embed_all()
```

### 4. Query with RAG
```python
from rag.query_medical_papers import ask

answer = ask(
    "What are the side effects of treatment X?",
    specialty="Cardiology"
)
print(answer)
```

## Comparing to Manual Setup

### Without Wizard (Manual)
- ❌ Manually create directories
- ❌ Copy-paste and edit boilerplate code
- ❌ Customize imports and schema names
- ❌ Create README documentation
- ⏱️ 15-20 minutes per project

### With Wizard (Automated)
- ✅ Answer 4 questions
- ✅ All files auto-generated with correct naming
- ✅ Metadata tables created with your fields
- ✅ Documentation auto-included
- ⏱️ 2 minutes per project

## File Generation Details

Each generated file includes:

**Ingestion Pipeline** (`load_*_docs.py`)
- Custom schema name matching project slug
- User-provided metadata fields in table definition
- Parameter passing for metadata during ingestion

**Embedding Module** (`embed_*_docs.py`)
- Dedicated ChromaDB collection per project
- Project slug in collection name
- Metadata enrichment from custom fields

**Query Module** (`query_*_docs.py`)
- Uses project-specific schema and collection
- Filter parameters matching user metadata
- Proper import statements with project slug

**README.md**
- Quick start guide
- Metadata field documentation
- Examples using project slug

## Extending the Generator

To add new features to generated projects:

1. Edit the generator function in `tools/pdf_project_generator.py`
2. All future projects will inherit the changes
3. Existing projects are unaffected

Example: Add dbt model templates
```python
def generate_dbt_model(project_name: str, project_slug: str) -> str:
    """Create sample dbt staging model."""
    return f"""SELECT * FROM documents_{project_slug}.chunks"""

# Add to create_project():
base / "dbt/lakehouse_mvp/models/staging" / f"stg_{project_slug}.sql":
    generate_dbt_model(project_name, project_slug)
```

## Troubleshooting

**Q: "tools" module not found**
A: Run from the lakehouse root directory:
```bash
cd /workspace
python notebooks/setup_pdf_project.py
```

**Q: Want to skip the wizard and use Python API directly**
A:
```python
from tools.pdf_project_generator import create_project
create_project(
    project_name="...",
    project_slug="...",
    doc_type="...",
    use_case="...",
    metadata_fields=[...]
)
```

**Q: Generated files have wrong names**
A: Check that `project_slug` contains only lowercase letters, numbers, and underscores.
The wizard auto-converts, but if calling Python API directly, ensure proper slugification.

## Summary

- **For quick project creation**: Use `python notebooks/setup_pdf_project.py`
- **For automation**: Import and call `create_project()` from Python
- **Reusable generator**: All projects inherit the same architecture
- **Extensible**: Add more generation functions as needed
