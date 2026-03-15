"""
PDF DATA PROJECT SETUP WIZARD
==============================

This script prompts for project requirements and generates all necessary files
for a new PDF ingestion + RAG project in the lakehouse.

It creates:
  - Directory structure
  - dlt ingestion pipeline
  - ChromaDB embedding module
  - RAG query module
  - Setup documentation

Usage:
  python tools/pdf_project_generator.py

Example output:
  Project name: my_research_papers
  Document type: Research papers
  Use case: Both (Recommended)
  Metadata: game_system, content_type

  → Creates all files automatically with correct naming/imports
"""

import re
from pathlib import Path
from datetime import datetime


def generate_ingest_pipeline(
    project_name: str,
    project_slug: str,
    doc_type: str,
    metadata_fields: list[str],
) -> str:
    """Generate dlt ingestion pipeline file."""
    metadata_doc = "\n".join([f"    {field}: Optional - {field.replace('_', ' ').title()}" for field in metadata_fields])
    metadata_params = ", ".join([f"{field}: str | None = None" for field in metadata_fields])
    metadata_insert = ", ".join([f"{field}" for field in metadata_fields])
    metadata_values = ", ".join(["?"] * len(metadata_fields))
    metadata_insert_params = ", ".join([f"{field}" for field in metadata_fields])

    return f'''"""
dlt pipeline: loads {project_name} PDFs into DuckDB (documents_{project_slug} schema).
Parses PDFs with Docling, chunks by headings, and stores with metadata.

Document Type: {doc_type}

Run from Jupyter:
  from dlt.load_{project_slug}_docs import run
  run({", ".join([f'{field}="value"' for field in metadata_fields[:1]])})
"""

import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
from docling.document_converter import DocumentConverter


DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/{project_slug}/raw")


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create documents_{project_slug} schema with chunks and metadata tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS documents_{project_slug}")

    # Chunks table: one row per chunk with full text + section info
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_{project_slug}.chunks (
            chunk_id        INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            section_title   VARCHAR,
            content         VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)

    # File metadata table: one row per document with tracking info
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_{project_slug}.files (
            source_file     VARCHAR PRIMARY KEY,
            document_title  VARCHAR,
{chr(10).join([f"            {field:<20} VARCHAR," for field in metadata_fields])}
            total_chunks    INTEGER NOT NULL,
            total_chars     INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)


def chunk_markdown(markdown: str, max_chars: int = 2000) -> list[dict]:
    """
    Split markdown by headings (H1-H4), then by paragraphs if sections exceed max_chars.
    Returns list of dicts with keys: section_title, content
    """
    sections = re.split(r"(?=^#{{1,4}}\\s)", markdown, flags=re.MULTILINE)
    chunks = []

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Extract heading as section title
        lines = section.split("\\n", 1)
        title = lines[0].lstrip("#").strip() if lines[0].startswith("#") else None

        # Single chunk if fits within max_chars
        if len(section) <= max_chars:
            chunks.append({{"section_title": title, "content": section}})
        else:
            # Split large sections by paragraphs
            paragraphs = section.split("\\n\\n")
            current = ""
            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    chunks.append({{"section_title": title, "content": current.strip()}})
                    current = para
                else:
                    current = current + "\\n\\n" + para if current else para
            if current.strip():
                chunks.append({{"section_title": title, "content": current.strip()}})

    return chunks


def ingest_file(
    filepath: Path,
    conn: duckdb.DuckDBPyConnection,
    document_title: str | None = None,
{chr(10).join([f"    {field}: str | None = None," for field in metadata_fields])}
) -> int:
    """Parse a single PDF and store chunks in DuckDB with metadata."""
    print(f"Parsing: {{filepath.name}}")

    try:
        converter = DocumentConverter()
        result = converter.convert(str(filepath))
        markdown = result.document.export_to_markdown()
    except Exception as e:
        print(f"  ERROR: Failed to parse {{filepath.name}}: {{e}}")
        return 0

    chunks = chunk_markdown(markdown)
    now = datetime.now(timezone.utc)

    # Remove old chunks and metadata for this file (re-ingest)
    conn.execute(
        "DELETE FROM documents_{project_slug}.chunks WHERE source_file = ?",
        [filepath.name],
    )
    conn.execute(
        "DELETE FROM documents_{project_slug}.files WHERE source_file = ?",
        [filepath.name],
    )

    # Get next chunk_id
    max_id = conn.execute(
        "SELECT COALESCE(MAX(chunk_id), 0) FROM documents_{project_slug}.chunks"
    ).fetchone()[0]

    # Insert chunks
    for i, chunk in enumerate(chunks):
        conn.execute(
            """INSERT INTO documents_{project_slug}.chunks
               (chunk_id, source_file, section_title, content, char_count, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [
                max_id + i + 1,
                filepath.name,
                chunk["section_title"],
                chunk["content"],
                len(chunk["content"]),
                now,
            ],
        )

    # Insert file metadata
    total_chars = sum(len(c["content"]) for c in chunks)
    conn.execute(
        """INSERT INTO documents_{project_slug}.files
           (source_file, document_title, {metadata_insert}, total_chunks, total_chars, parsed_at)
           VALUES (?, ?, {metadata_values}, ?, ?, ?)""",
        [
            filepath.name,
            document_title or filepath.stem,
{chr(10).join([f"            {field}," for field in metadata_insert_params])}
            len(chunks),
            total_chars,
            now,
        ],
    )

    print(f"  → {{len(chunks)}} chunks, {{total_chars:,}} chars")
    return len(chunks)


def ingest_all(
    directory: Path | None = None,
{metadata_params},
) -> None:
    """
    Parse all PDFs in documents/{project_slug}/raw directory.

    Args:
        directory: Override directory to scan
{metadata_doc}
    """
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))

    if not files:
        print(f"No PDF files found in {{doc_dir}}")
        return

    conn = duckdb.connect(DB_PATH)
    init_schema(conn)

    total_chunks = 0
    for f in files:
        total_chunks += ingest_file(
            f,
            conn,
{chr(10).join([f"            {field}={field}," for field in metadata_fields])}
        )

    conn.close()
    print(f"\\nDone: {{len(files)}} files, {{total_chunks}} total chunks ingested")


def run(
{metadata_params},
) -> None:
    """Entrypoint for Jupyter."""
    ingest_all({", ".join([f"{field}={field}" for field in metadata_fields])})


if __name__ == "__main__":
    # Example usage
    run({", ".join([f'{field}="example"' for field in metadata_fields[:1]])})
'''


def generate_embed_module(project_name: str, project_slug: str) -> str:
    """Generate ChromaDB embedding module."""
    return f'''"""
Embed {project_name} document chunks from DuckDB into ChromaDB for semantic search.
Creates and maintains a dedicated collection: '{project_slug}_chunks'

Run from Jupyter:
  from rag.embed_{project_slug} import embed_all
  embed_all()
"""

import duckdb
import chromadb
from chromadb.config import Settings

DB_PATH = "/workspace/db/lakehouse.duckdb"
CHROMA_PATH = "/workspace/chroma_db"
COLLECTION_NAME = "{project_slug}_chunks"


def embed_all() -> None:
    """Read chunks from documents_{project_slug} schema and upsert into ChromaDB."""
    conn = duckdb.connect(DB_PATH, read_only=True)

    # Query all chunks with their metadata
    try:
        rows = conn.execute("""
            SELECT
                c.chunk_id,
                c.source_file,
                c.section_title,
                c.content,
                f.document_title,
                f.*
            FROM documents_{project_slug}.chunks c
            LEFT JOIN documents_{project_slug}.files f ON c.source_file = f.source_file
            ORDER BY c.chunk_id
        """).fetchall()
    except Exception as e:
        print(f"Query error: {{e}}")
        rows = []
    finally:
        conn.close()

    if not rows:
        print("No chunks found in documents_{project_slug} schema.")
        print("Run: from dlt.load_{project_slug}_docs import run; run()")
        return

    # Initialize ChromaDB client
    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )

    # Get or create dedicated collection
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={{"hnsw:space": "cosine", "project": "{project_slug}"}},
    )

    # Prepare batch data
    ids = [str(r[0]) for r in rows]
    documents = [r[3] for r in rows]
    metadatas = [
        {{
            "source_file": r[1],
            "section_title": r[2] or "",
            "chunk_id": str(r[0]),
        }}
        for r in rows
    ]

    # Upsert in batches
    batch_size = 100
    for i in range(0, len(ids), batch_size):
        batch_end = min(i + batch_size, len(ids))
        collection.upsert(
            ids=ids[i:batch_end],
            documents=documents[i:batch_end],
            metadatas=metadatas[i:batch_end],
        )
        print(f"  Embedded chunks {{i + 1}}–{{batch_end}} of {{len(ids)}}")

    print(f"\\nDone: {{len(ids)}} chunks embedded in ChromaDB collection '{{COLLECTION_NAME}}'")
    print(f"Collection count: {{collection.count()}}")


if __name__ == "__main__":
    embed_all()
'''


def generate_query_module(
    project_name: str,
    project_slug: str,
    metadata_fields: list[str],
) -> str:
    """Generate RAG query module."""
    filter_params = "\n        ".join([f"{field}: Optional[str] = None," for field in metadata_fields])
    filter_docs = "\n        ".join([f"{field}: Optional filter by {field}" for field in metadata_fields])

    return f'''"""
RAG query engine for {project_name}: retrieve from project-specific schema and collections.
Supports semantic search (ChromaDB) + keyword search (DuckDB) with project filtering.

Run from Jupyter:
  from rag.query_{project_slug} import ask
  answer = ask("Your question here")
"""

import duckdb
import chromadb
from chromadb.config import Settings
import requests
from typing import Optional

DB_PATH = "/workspace/db/lakehouse.duckdb"
CHROMA_PATH = "/workspace/chroma_db"
COLLECTION_NAME = "{project_slug}_chunks"
OLLAMA_URL = "http://host.docker.internal:11434"
DEFAULT_MODEL = "llama3:70b"


def search_chromadb(
    query: str,
    n_results: int = 5,
{filter_params}
) -> list[dict]:
    """Semantic search over {project_name} chunks."""
    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )

    try:
        collection = client.get_collection(COLLECTION_NAME)
    except Exception:
        return []

    results = collection.query(query_texts=[query], n_results=n_results)

    chunks = []
    for i in range(len(results["ids"][0])):
        meta = results["metadatas"][0][i]
        chunks.append(
            {{
                "content": results["documents"][0][i],
                "source_file": meta.get("source_file", ""),
                "section_title": meta.get("section_title", ""),
                "distance": results["distances"][0][i],
            }}
        )
    return chunks


def search_duckdb(
    query: str,
    limit: int = 5,
{filter_params}
) -> list[dict]:
    """Keyword search over chunks in documents_{project_slug} schema."""
    conn = duckdb.connect(DB_PATH, read_only=True)

    words = query.lower().split()
    keyword_clauses = [f"LOWER(c.content) LIKE '%{{w}}%'" for w in words if len(w) > 2]
    where = f"WHERE {{' AND '.join(keyword_clauses)}}" if keyword_clauses else ""

    sql = f"""
        SELECT c.source_file, c.section_title, c.content,
               LENGTH(c.content) as char_count
        FROM documents_{project_slug}.chunks c
        {{where}}
        ORDER BY char_count
        LIMIT {{limit}}
    """

    try:
        rows = conn.execute(sql).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    return [
        {{
            "content": r[2],
            "source_file": r[0],
            "section_title": r[1] or "",
            "distance": 0.0,
        }}
        for r in rows
    ]


def retrieve_context(
    query: str,
    n_results: int = 5,
{filter_params}
) -> list[dict]:
    """Retrieve context using hybrid search (semantic + keyword)."""
    semantic = search_chromadb(query, n_results=n_results)
    keyword = search_duckdb(query, limit=n_results)

    # Deduplicate by content
    seen = set()
    combined = []
    for chunk in semantic + keyword:
        key = chunk["content"][:200]
        if key not in seen:
            seen.add(key)
            combined.append(chunk)

    return combined[:n_results]


def ask(
    question: str,
    model: str = DEFAULT_MODEL,
    n_results: int = 5,
    show_sources: bool = True,
{filter_params}
) -> str:
    """Answer a question using retrieved document context + LLM."""
    chunks = retrieve_context(question, n_results=n_results)

    if not chunks:
        return (
            "No relevant documents found. "
            "Please ingest PDFs first: run load_{project_slug}_docs.py and embed_{project_slug}.py"
        )

    context = "\\n\\n---\\n\\n".join(
        f"[{{c['source_file']}} | {{c['section_title']}}]\\n{{c['content']}}"
        for c in chunks
    )

    prompt = f"""You are a helpful assistant for {project_name}.
Answer the question based ONLY on the provided context.
If the answer is not in the context, say so clearly.

CONTEXT:
{{context}}

QUESTION: {{question}}

ANSWER:"""

    try:
        response = requests.post(
            f"{{OLLAMA_URL}}/api/generate",
            json={{"model": model, "prompt": prompt, "stream": False}},
            timeout=120,
        )
        response.raise_for_status()
        answer = response.json()["response"]
    except requests.exceptions.RequestException as e:
        return f"Error calling LLM: {{e}}"

    if show_sources:
        sources = "\\n".join(f"  - {{c['source_file']}}: {{c['section_title']}}" for c in chunks)
        answer += f"\\n\\nSources:\\n{{sources}}"

    return answer


if __name__ == "__main__":
    import sys
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What information do you have?"
    print(ask(question))
'''


def generate_readme(
    project_name: str,
    project_slug: str,
    doc_type: str,
    metadata_fields: list[str],
) -> str:
    """Generate README for the project."""
    return f'''# {project_name}

PDF storage and RAG-enabled search for {doc_type.lower()}.

## Directory Structure

```
{project_slug}/
├── raw/           ← Place your PDF files here
├── processed/     ← Reserved for preprocessing
└── README.md      ← This file
```

## Quick Start

### 1. Add PDFs
Copy your PDF files to `raw/` folder.

### 2. Ingest (In Jupyter)
```python
from dlt.load_{project_slug}_docs import run
run()
```

### 3. Embed
```python
from rag.embed_{project_slug} import embed_all
embed_all()
```

### 4. Query
```python
from rag.query_{project_slug} import ask
answer = ask("Your question here")
print(answer)
```

## Metadata Tracked

{chr(10).join([f"- `{field}`" for field in metadata_fields])}

## Related Files

- **Ingestion**: `dlt/load_{project_slug}_docs.py`
- **Embedding**: `rag/embed_{project_slug}.py`
- **Querying**: `rag/query_{project_slug}.py`
'''


def create_project(
    project_name: str,
    project_slug: str,
    doc_type: str,
    use_case: str,
    metadata_fields: list[str],
):
    """Generate all files for a new PDF project."""
    base = Path("/workspace")

    # Create directories
    project_dir = base / "documents" / project_slug
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "raw").mkdir(exist_ok=True)
    (project_dir / "processed").mkdir(exist_ok=True)

    # Generate and write files
    files_to_create = {
        base / "dlt" / f"load_{project_slug}_docs.py": generate_ingest_pipeline(
            project_name, project_slug, doc_type, metadata_fields
        ),
        base / "rag" / f"embed_{project_slug}.py": generate_embed_module(
            project_name, project_slug
        ),
        base / "rag" / f"query_{project_slug}.py": generate_query_module(
            project_name, project_slug, metadata_fields
        ),
        project_dir / "README.md": generate_readme(
            project_name, project_slug, doc_type, metadata_fields
        ),
    }

    for filepath, content in files_to_create.items():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content)
        print(f"✓ Created {filepath}")

    print(f"\n✅ Project '{project_name}' created successfully!")
    print(f"\nNext steps:")
    print(f"1. Add PDFs to: documents/{project_slug}/raw/")
    print(f"2. Run ingestion: from dlt.load_{project_slug}_docs import run; run()")
    print(f"3. Run embedding: from rag.embed_{project_slug} import embed_all; embed_all()")
    print(f"4. Ask questions: from rag.query_{project_slug} import ask; ask('your question')")


if __name__ == "__main__":
    # Example usage - would normally come from AskUserQuestion
    create_project(
        project_name="Test Project",
        project_slug="test_project",
        doc_type="Research papers",
        use_case="Both (Recommended)",
        metadata_fields=["field1", "field2"],
    )
