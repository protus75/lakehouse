"""
Embed tabletop_rules document chunks from DuckDB into ChromaDB for semantic search.
Creates and maintains a dedicated collection: 'tabletop_rules_chunks'

Run from Jupyter:
  from rag.embed_tabletop_rules import embed_all
  embed_all()
"""

import duckdb
import chromadb
from chromadb.config import Settings

DB_PATH = "/workspace/db/lakehouse.duckdb"
CHROMA_PATH = "/workspace/chroma_db"
COLLECTION_NAME = "tabletop_rules_chunks"


def embed_all() -> None:
    """Read chunks from documents_tabletop_rules schema and upsert into ChromaDB."""
    conn = duckdb.connect(DB_PATH, read_only=True)

    # Query all chunks with their metadata
    rows = conn.execute("""
        SELECT
            c.chunk_id,
            c.source_file,
            c.section_title,
            c.content,
            f.document_title,
            f.game_system,
            f.content_type,
            f.tags
        FROM documents_tabletop_rules.chunks c
        LEFT JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
        ORDER BY c.chunk_id
    """).fetchall()
    conn.close()

    if not rows:
        print("No chunks found in documents_tabletop_rules schema.")
        print("Run: from dlt.load_tabletop_rules_docs import run; run()")
        return

    # Initialize ChromaDB client
    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )

    # Get or create dedicated collection for tabletop_rules
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine", "project": "tabletop_rules"},
    )

    # Prepare batch data
    ids = [str(r[0]) for r in rows]
    documents = [r[3] for r in rows]
    metadatas = [
        {
            "source_file": r[1],
            "section_title": r[2] or "",
            "chunk_id": str(r[0]),
            "document_title": r[4] or "",
            "game_system": r[5] or "",
            "content_type": r[6] or "",
            "tags": r[7] or "",
        }
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
        print(f"  Embedded chunks {i + 1}–{batch_end} of {len(ids)}")

    print(f"\nDone: {len(ids)} chunks embedded in ChromaDB collection '{COLLECTION_NAME}'")
    print(f"Collection count: {collection.count()}")


if __name__ == "__main__":
    embed_all()
