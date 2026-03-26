"""Parse PDFs with Docling and store chunks in DuckDB + ChromaDB."""

import re
from pathlib import Path
from datetime import datetime, timezone

import sys
sys.path.insert(0, "/workspace")
import duckdb
from dlt.lib.iceberg_catalog import write_iceberg
from docling.document_converter import DocumentConverter
DOCUMENTS_DIR = Path("/workspace/documents")


def init_documents_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS documents")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents.chunks (
            chunk_id        INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            page_start      INTEGER,
            section_title   VARCHAR,
            content         VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents.files (
            source_file     VARCHAR PRIMARY KEY,
            total_chunks    INTEGER NOT NULL,
            total_chars     INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)


def chunk_markdown(markdown: str, max_chars: int = 2000) -> list[dict]:
    """Split markdown by headings, then by size if sections are too large."""
    sections = re.split(r"(?=^#{1,4}\s)", markdown, flags=re.MULTILINE)
    chunks = []

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Extract heading as section title
        lines = section.split("\n", 1)
        title = lines[0].lstrip("#").strip() if lines[0].startswith("#") else None

        if len(section) <= max_chars:
            chunks.append({"section_title": title, "content": section})
        else:
            # Split large sections by paragraphs
            paragraphs = section.split("\n\n")
            current = ""
            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    chunks.append({"section_title": title, "content": current.strip()})
                    current = para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append({"section_title": title, "content": current.strip()})

    return chunks


def ingest_file(filepath: Path, conn: duckdb.DuckDBPyConnection) -> int:
    """Parse a single document and store chunks in DuckDB."""
    print(f"Parsing: {filepath.name}")
    converter = DocumentConverter()
    result = converter.convert(str(filepath))
    markdown = result.document.export_to_markdown()

    chunks = chunk_markdown(markdown)
    now = datetime.now(timezone.utc)

    # Remove old chunks for this file (re-ingest)
    conn.execute(
        "DELETE FROM documents.chunks WHERE source_file = ?", [filepath.name]
    )
    conn.execute(
        "DELETE FROM documents.files WHERE source_file = ?", [filepath.name]
    )

    # Get next chunk_id
    max_id = conn.execute(
        "SELECT COALESCE(MAX(chunk_id), 0) FROM documents.chunks"
    ).fetchone()[0]

    for i, chunk in enumerate(chunks):
        conn.execute(
            """INSERT INTO documents.chunks
               (chunk_id, source_file, page_start, section_title, content, char_count, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                max_id + i + 1,
                filepath.name,
                None,
                chunk["section_title"],
                chunk["content"],
                len(chunk["content"]),
                now,
            ],
        )

    conn.execute(
        """INSERT INTO documents.files (source_file, total_chunks, total_chars, parsed_at)
           VALUES (?, ?, ?, ?)""",
        [filepath.name, len(chunks), sum(len(c["content"]) for c in chunks), now],
    )

    print(f"  → {len(chunks)} chunks, {sum(len(c['content']) for c in chunks):,} chars")
    return len(chunks)


def ingest_all(directory: Path | None = None) -> None:
    """Parse all PDFs in the documents directory."""
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))

    if not files:
        print(f"No PDF files found in {doc_dir}")
        return

    conn = duckdb.connect()  # deprecated: use bronze pipeline instead
    init_documents_table(conn)

    total_chunks = 0
    for f in files:
        total_chunks += ingest_file(f, conn)

    conn.close()
    print(f"\nDone: {len(files)} files, {total_chunks} total chunks")


if __name__ == "__main__":
    ingest_all()
