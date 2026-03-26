"""
Embed tabletop_rules document chunks from DuckDB into ChromaDB.
Includes toc_id in metadata for filtered section-level search.
"""

import sys
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
import chromadb
from chromadb.config import Settings
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

CHROMA_PATH = "/workspace/chroma_db"
COLLECTION_NAME = "tabletop_rules_chunks"
EMBEDDING_MODEL = "all-mpnet-base-v2"


def embed_all() -> None:
    """Read chunks from DuckDB and upsert into ChromaDB with toc metadata."""
    import time
    start = time.time()

    conn = get_reader()
    rows = conn.execute("""
        SELECT
            c.chunk_id,
            c.source_file,
            c.toc_id,
            c.section_title,
            c.entry_title,
            c.content,
            c.page_numbers,
            c.chunk_type,
            t.title as toc_title,
            f.game_system,
            f.content_type
        FROM documents_tabletop_rules.chunks c
        LEFT JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
        LEFT JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
        ORDER BY c.chunk_id
    """).fetchall()
    conn.close()

    if not rows:
        print("No chunks found. Run ingestion first.")
        return

    print(f"Embedding {len(rows)} chunks into ChromaDB...")

    client = chromadb.PersistentClient(
        path=CHROMA_PATH, settings=Settings(anonymized_telemetry=False),
    )
    embedding_fn = SentenceTransformerEmbeddingFunction(model_name=EMBEDDING_MODEL)
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine", "project": "tabletop_rules"},
        embedding_function=embedding_fn,
    )

    ids = [str(r[0]) for r in rows]
    documents = [r[5] for r in rows]
    metadatas = [{
        "source_file": r[1],
        "toc_id": str(r[2] or ""),
        "section_title": r[3] or "",
        "entry_title": r[4] or "",
        "page_numbers": r[6] or "",
        "chunk_type": r[7] or "content",
        "toc_title": r[8] or "",
        "game_system": r[9] or "",
        "content_type": r[10] or "",
    } for r in rows]

    batch_size = 100
    embed_start = time.time()
    for i in range(0, len(ids), batch_size):
        end = min(i + batch_size, len(ids))
        collection.upsert(
            ids=ids[i:end],
            documents=documents[i:end],
            metadatas=metadatas[i:end],
        )
        print(f"  Embedded {end}/{len(ids)} chunks")

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s: {len(ids)} chunks embedded")


if __name__ == "__main__":
    embed_all()
