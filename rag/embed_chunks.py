"""Embed document chunks from DuckDB into ChromaDB for semantic search."""

import sys
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
import chromadb
from chromadb.config import Settings

CHROMA_PATH = "/workspace/chroma_db"


def embed_all() -> None:
    """Read chunks from DuckDB and upsert into ChromaDB with embeddings."""
    conn = get_reader()
    rows = conn.execute("""
        SELECT chunk_id, source_file, section_title, content
        FROM documents.chunks
        ORDER BY chunk_id
    """).fetchall()
    conn.close()

    if not rows:
        print("No chunks found in DuckDB. Run ingest_documents.py first.")
        return

    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )

    collection = client.get_or_create_collection(
        name="document_chunks",
        metadata={"hnsw:space": "cosine"},
    )

    ids = [str(r[0]) for r in rows]
    documents = [r[3] for r in rows]
    metadatas = [
        {"source_file": r[1], "section_title": r[2] or "", "chunk_id": r[0]}
        for r in rows
    ]

    # ChromaDB uses its default embedding function (all-MiniLM-L6-v2)
    # For GPU-accelerated embeddings, configure sentence-transformers separately
    batch_size = 100
    for i in range(0, len(ids), batch_size):
        batch_end = min(i + batch_size, len(ids))
        collection.upsert(
            ids=ids[i:batch_end],
            documents=documents[i:batch_end],
            metadatas=metadatas[i:batch_end],
        )
        print(f"  Embedded chunks {i + 1}–{batch_end} of {len(ids)}")

    print(f"\nDone: {len(ids)} chunks embedded in ChromaDB")
    print(f"Collection count: {collection.count()}")


if __name__ == "__main__":
    embed_all()
