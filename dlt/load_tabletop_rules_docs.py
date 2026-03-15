"""
dlt pipeline: loads tabletop RPG/board game PDFs into DuckDB (documents_tabletop_rules schema).
Parses PDFs with Docling, chunks by headings, and stores with metadata.

Run from Jupyter:
  from dlt.load_tabletop_rules_docs import run
  run(game_system="D&D 5e", content_type="rules")
"""

import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import duckdb
from docling.document_converter import DocumentConverter


DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create documents_tabletop_rules schema with chunks and metadata tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS documents_tabletop_rules")

    # Chunks table: one row per chunk with full text + section info
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.chunks (
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
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.files (
            source_file     VARCHAR PRIMARY KEY,
            document_title  VARCHAR,
            game_system     VARCHAR,
            content_type    VARCHAR,
            tags            VARCHAR,
            rules_version   VARCHAR,
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
    # Split on markdown headings (# ## ### ####)
    sections = re.split(r"(?=^#{1,4}\s)", markdown, flags=re.MULTILINE)
    chunks = []

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Extract heading as section title
        lines = section.split("\n", 1)
        title = lines[0].lstrip("#").strip() if lines[0].startswith("#") else None

        # Single chunk if fits within max_chars
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


def ingest_file(
    filepath: Path,
    conn: duckdb.DuckDBPyConnection,
    document_title: str | None = None,
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
    rules_version: str | None = None,
) -> int:
    """Parse a single PDF and store chunks in DuckDB with metadata."""
    print(f"Parsing: {filepath.name}")

    try:
        converter = DocumentConverter()
        result = converter.convert(str(filepath))
        markdown = result.document.export_to_markdown()
    except Exception as e:
        print(f"  ERROR: Failed to parse {filepath.name}: {e}")
        return 0

    chunks = chunk_markdown(markdown)
    now = datetime.now(timezone.utc)

    # Remove old chunks and metadata for this file (re-ingest)
    conn.execute(
        "DELETE FROM documents_tabletop_rules.chunks WHERE source_file = ?",
        [filepath.name],
    )
    conn.execute(
        "DELETE FROM documents_tabletop_rules.files WHERE source_file = ?",
        [filepath.name],
    )

    # Get next chunk_id
    max_id = conn.execute(
        "SELECT COALESCE(MAX(chunk_id), 0) FROM documents_tabletop_rules.chunks"
    ).fetchone()[0]

    # Insert chunks
    for i, chunk in enumerate(chunks):
        conn.execute(
            """INSERT INTO documents_tabletop_rules.chunks
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
        """INSERT INTO documents_tabletop_rules.files
           (source_file, document_title, game_system, content_type, tags, rules_version,
            total_chunks, total_chars, parsed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            filepath.name,
            document_title or filepath.stem,
            game_system,
            content_type,
            tags,
            rules_version,
            len(chunks),
            total_chars,
            now,
        ],
    )

    print(f"  → {len(chunks)} chunks, {total_chars:,} chars")
    return len(chunks)


def ingest_all(
    directory: Path | None = None,
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
) -> None:
    """
    Parse all PDFs in documents/tabletop_rules/raw directory.

    Args:
        directory: Override directory to scan (defaults to /workspace/documents/tabletop_rules/raw)
        game_system: e.g., "D&D 5e", "Pathfinder 2e"
        content_type: e.g., "rules", "module", "campaign"
        tags: comma-separated tags for categorization
    """
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))

    if not files:
        print(f"No PDF files found in {doc_dir}")
        return

    conn = duckdb.connect(DB_PATH)
    init_schema(conn)

    total_chunks = 0
    for f in files:
        total_chunks += ingest_file(
            f,
            conn,
            game_system=game_system,
            content_type=content_type,
            tags=tags,
        )

    conn.close()
    print(f"\nDone: {len(files)} files, {total_chunks} total chunks ingested")


def run(
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
) -> None:
    """Entrypoint for Jupyter."""
    ingest_all(game_system=game_system, content_type=content_type, tags=tags)


if __name__ == "__main__":
    # Example: ingest all PDFs as D&D 5e rules
    run(game_system="D&D 5e", content_type="rules")
