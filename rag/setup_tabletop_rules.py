"""
TABLETOP RULES PROJECT SETUP & DOCUMENTATION

Setup script for ingesting and searching tabletop RPG/board game rules PDFs.

Project Structure:
  documents/tabletop_rules/
  ├── raw/              ← Place your PDF files here
  ├── processed/        ← Reserved for future preprocessing
  └── .gitkeep

Database:
  lakehouse.duckdb/
  └── documents_tabletop_rules/
      ├── chunks       ← Document text chunks with metadata
      └── files        ← File-level metadata and tracking

Vector Store:
  chroma_db/
  └── tabletop_rules_chunks  ← Embeddings for semantic search

Metadata Fields Tracked:
  - filename: Original PDF filename
  - document_title: Title (auto-extracted from filename if not provided)
  - game_system: D&D 5e, Pathfinder 2e, etc.
  - content_type: rules, module, campaign, etc.
  - tags: Comma-separated categories
  - rules_version: Version number of rules
  - dates: Ingestion timestamp (UTC)

QUICKSTART
==========

1. Place PDFs in documents/tabletop_rules/raw/

2. From Jupyter, run in order:

   # Step 1: Ingest PDFs and parse into chunks
   from dlt.load_tabletop_rules_docs import run as ingest
   ingest(game_system="D&D 5e", content_type="rules")

   # Step 2: Embed chunks into ChromaDB for semantic search
   from rag.embed_tabletop_rules import embed_all
   embed_all()

   # Step 3: Ask questions about the rules
   from rag.query_tabletop_rules import ask
   answer = ask("What are the rules for attacking?", game_system="D&D 5e")
   print(answer)

3. Check ingestion status:
   from rag.query_tabletop_rules import search_duckdb
   results = search_duckdb("shield spell")
   for r in results[:3]:
       print(f"{r['source_file']}: {r['section_title']}")

DETAILED WORKFLOW
=================

A. INGESTION (load_tabletop_rules_docs.py)
   ├─ Scans documents/tabletop_rules/raw/ for PDFs
   ├─ Uses Docling library to extract text from PDFs
   ├─ Chunks markdown by headings (H1-H4), then by paragraphs (~2000 char chunks)
   ├─ Stores chunks in DuckDB with metadata
   └─ Creates documents_tabletop_rules schema on first run

   Usage:
     from dlt.load_tabletop_rules_docs import ingest_all
     ingest_all(
         game_system="D&D 5e",
         content_type="rules",
         tags="5th-edition,combat"
     )

B. EMBEDDING (embed_tabletop_rules.py)
   ├─ Reads chunks from DuckDB
   ├─ Generates embeddings (all-MiniLM-L6-v2 via ChromaDB)
   ├─ Stores embeddings in dedicated "tabletop_rules_chunks" collection
   └─ Includes rich metadata (game_system, content_type, tags, etc.)

   Usage:
     from rag.embed_tabletop_rules import embed_all
     embed_all()

C. RETRIEVAL & Q&A (query_tabletop_rules.py)
   ├─ Semantic search: User question → embeddings → ChromaDB cosine search
   ├─ Keyword search: Keyword matching in DuckDB chunks
   ├─ Hybrid retrieval: Combine both, deduplicate by content
   ├─ Context building: Format chunks with full metadata
   └─ LLM answering: Send context + question to Ollama (llama3:70b)

   Usage - Basic:
     from rag.query_tabletop_rules import ask
     answer = ask("How do you calculate armor class?")
     print(answer)

   Usage - Filtered by game system:
     answer = ask(
         "How do you resolve attacks?",
         game_system="D&D 5e",
         content_type="rules"
     )

   Usage - Keyword search only:
     from rag.query_tabletop_rules import search_duckdb
     results = search_duckdb("saving throw", game_system="D&D 5e")

   Usage - Semantic search only:
     from rag.query_tabletop_rules import search_chromadb
     results = search_chromadb("spell duration", game_system="D&D 5e")

DATABASE SCHEMA
===============

Table: documents_tabletop_rules.chunks
  chunk_id        INTEGER PRIMARY KEY
  source_file     VARCHAR NOT NULL            ← PDF filename
  section_title   VARCHAR                     ← Extracted heading
  content         VARCHAR NOT NULL            ← Actual text
  char_count      INTEGER NOT NULL            ← Content length
  parsed_at       TIMESTAMP NOT NULL          ← Parse time (UTC)

Table: documents_tabletop_rules.files
  source_file     VARCHAR PRIMARY KEY         ← PDF filename
  document_title  VARCHAR                     ← Title
  game_system     VARCHAR                     ← D&D 5e, Pathfinder 2e, etc.
  content_type    VARCHAR                     ← rules/module/campaign
  tags            VARCHAR                     ← Comma-separated tags
  rules_version   VARCHAR                     ← Version number
  total_chunks    INTEGER NOT NULL            ← # of chunks
  total_chars     INTEGER NOT NULL            ← Total characters
  parsed_at       TIMESTAMP NOT NULL          ← Ingest time (UTC)

QUERY EXAMPLES
==============

1. Find all rules documents for D&D 5e:
   from rag.query_tabletop_rules import search_duckdb
   results = search_duckdb("", game_system="D&D 5e", content_type="rules")
   # Returns metadata about all chunks (not useful without query filter)

2. Search for spellcasting across all books:
   answer = ask("How do you cast a spell?")

3. Search only in D&D 5e Player's Handbook:
   answer = ask("How do you multiclass?", game_system="D&D 5e")

4. Search only campaign material:
   answer = ask("What happens in Act 1?", content_type="campaign")

5. Find specific sections:
   from rag.query_tabletop_rules import search_chromadb
   results = search_chromadb("concentration check", n_results=10)

TROUBLESHOOTING
===============

Q: "No chunks found in documents_tabletop_rules schema"
A: Run ingest first:
   from dlt.load_tabletop_rules_docs import run
   run(game_system="D&D 5e", content_type="rules")

Q: "ConnectionRefusedError: [Errno 10061] No connection could be made"
A: Ollama is not running. Start it:
   Windows: ollama serve
   Or Docker: docker compose up

Q: Embeddings not updating after adding new PDFs
A: Re-embed after re-ingesting:
   from dlt.load_tabletop_rules_docs import run
   from rag.embed_tabletop_rules import embed_all
   run(); embed_all()

Q: Search results not relevant
A: Try keyword search instead of semantic:
   from rag.query_tabletop_rules import search_duckdb
   results = search_duckdb("shield spell")

NEXT STEPS
==========

1. Add dbt models for analytics (optional):
   dbt/lakehouse_mvp/models/staging/documents/
   ├── stg_tabletop_rules_chunks.sql
   └── stg_tabletop_rules_files.sql

2. Create dashboard for document statistics:
   streamlit/dashboard_rules.py
   ├── Document count by game system
   ├── Content type distribution
   ├── Search frequency analysis

3. Expand query module:
   ├── Add filters for multiple game systems
   ├── Batch processing for large question sets
   ├── Response caching

4. Multi-project support:
   ├── Extend BaseProjectConfig class
   ├── Create project factory pattern
   ├── Unified query interface for multiple projects

"""

from pathlib import Path


def print_setup_guide():
    """Print the full setup guide."""
    print(__doc__)


def verify_directories():
    """Check if required directories exist."""
    required_dirs = [
        Path("/workspace/documents/tabletop_rules/raw"),
        Path("/workspace/documents/tabletop_rules/processed"),
    ]

    for d in required_dirs:
        if d.exists():
            print(f"✓ {d}")
        else:
            print(f"✗ {d} (missing)")


def verify_imports():
    """Check if required packages are importable."""
    packages = [
        ("duckdb", "DuckDB"),
        ("docling", "Docling"),
        ("chromadb", "ChromaDB"),
        ("requests", "Requests"),
    ]

    for module, name in packages:
        try:
            __import__(module)
            print(f"✓ {name}")
        except ImportError:
            print(f"✗ {name} (not installed)")


def quick_status():
    """Print quick status of project setup."""
    print("\n=== TABLETOP RULES PROJECT STATUS ===\n")

    print("Directories:")
    verify_directories()

    print("\nRequired Packages:")
    verify_imports()

    print("\n=== NEXT STEPS ===")
    print("1. Place PDF files in: documents/tabletop_rules/raw/")
    print("2. Run ingestion: from dlt.load_tabletop_rules_docs import run; run()")
    print("3. Run embedding: from rag.embed_tabletop_rules import embed_all; embed_all()")
    print("4. Ask questions: from rag.query_tabletop_rules import ask; ask('your question')")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        quick_status()
    else:
        print_setup_guide()
