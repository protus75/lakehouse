"""RAG query engine: retrieve document context and answer questions via LLM."""

import sys
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
import chromadb
from chromadb.config import Settings
import requests

CHROMA_PATH = "/workspace/chroma_db"
OLLAMA_URL = "http://host.docker.internal:11434"
DEFAULT_MODEL = "llama3:70b"


def search_chromadb(query: str, n_results: int = 5) -> list[dict]:
    """Semantic search over document chunks."""
    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )
    collection = client.get_collection("document_chunks")
    results = collection.query(query_texts=[query], n_results=n_results)

    chunks = []
    for i in range(len(results["ids"][0])):
        chunks.append({
            "content": results["documents"][0][i],
            "source_file": results["metadatas"][0][i]["source_file"],
            "section_title": results["metadatas"][0][i]["section_title"],
            "distance": results["distances"][0][i],
        })
    return chunks


def search_duckdb(query: str, limit: int = 5) -> list[dict]:
    """Keyword search over document chunks in DuckDB."""
    conn = get_reader()
    # Simple keyword search using LIKE with each word
    words = query.lower().split()
    where_clauses = [f"LOWER(content) LIKE '%{w}%'" for w in words if len(w) > 2]

    if not where_clauses:
        conn.close()
        return []

    sql = f"""
        SELECT source_file, section_title, content,
               LENGTH(content) as char_count
        FROM documents.chunks
        WHERE {' AND '.join(where_clauses)}
        ORDER BY char_count
        LIMIT {limit}
    """
    rows = conn.execute(sql).fetchall()
    conn.close()

    return [
        {
            "content": r[2],
            "source_file": r[0],
            "section_title": r[1] or "",
            "distance": 0.0,
        }
        for r in rows
    ]


def retrieve_context(query: str, n_results: int = 5) -> list[dict]:
    """Combine semantic (ChromaDB) and keyword (DuckDB) search results."""
    semantic = search_chromadb(query, n_results=n_results)
    keyword = search_duckdb(query, limit=n_results)

    # Deduplicate by content, preferring semantic results
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
) -> str:
    """Answer a question using retrieved document context + LLM."""
    chunks = retrieve_context(question, n_results=n_results)

    if not chunks:
        return "No relevant documents found. Please ingest documents first."

    context = "\n\n---\n\n".join(
        f"[Source: {c['source_file']} | Section: {c['section_title']}]\n{c['content']}"
        for c in chunks
    )

    prompt = f"""You are a helpful assistant that answers questions based on the provided document context.
Use ONLY the information in the context below to answer. If the answer is not in the context, say so.
Cite the source document and section when possible.

CONTEXT:
{context}

QUESTION: {question}

ANSWER:"""

    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    answer = response.json()["response"]

    if show_sources:
        sources = "\n".join(
            f"  - {c['source_file']}: {c['section_title']}" for c in chunks
        )
        answer += f"\n\nSources:\n{sources}"

    return answer


if __name__ == "__main__":
    import sys

    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What rules apply?"
    print(ask(question))
