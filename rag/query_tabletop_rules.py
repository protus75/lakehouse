"""
RAG query engine for tabletop_rules: retrieve from project-specific schema and collections.
Supports semantic search (ChromaDB) + keyword search (DuckDB) with project filtering.

Run from Jupyter:
  from rag.query_tabletop_rules import ask
  answer = ask("What are the rules for attacking?", game_system="D&D 2e")

Or with FastAPI:
  python rag/query_tabletop_rules.py --question "How do spells work?"
"""

import duckdb
import chromadb
from chromadb.config import Settings
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
import requests
from typing import Optional

DB_PATH = "/workspace/db/lakehouse.duckdb"
CHROMA_PATH = "/workspace/chroma_db"
COLLECTION_NAME = "tabletop_rules_chunks"
EMBEDDING_MODEL = "all-mpnet-base-v2"
OLLAMA_URL = "http://host.docker.internal:11434"  # Native Windows host
DEFAULT_MODEL = "llama3:70b"


def search_chromadb(
    query: str,
    n_results: int = 5,
    game_system: Optional[str] = None,
    content_type: Optional[str] = None,
) -> list[dict]:
    """
    Semantic search over tabletop_rules chunks.

    Args:
        query: The search question
        n_results: Number of results to return
        game_system: Filter by game system (e.g., "D&D 2e")
        content_type: Filter by type (e.g., "rules", "module", "campaign")
    """
    client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False),
    )

    embedding_fn = SentenceTransformerEmbeddingFunction(model_name=EMBEDDING_MODEL)

    try:
        collection = client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)
    except Exception:
        return []

    # Build where filter
    where_filter = None
    if game_system or content_type:
        conditions = []
        if game_system:
            conditions.append({"game_system": {"$eq": game_system}})
        if content_type:
            conditions.append({"content_type": {"$eq": content_type}})

        if conditions:
            where_filter = {"$and": conditions} if len(conditions) > 1 else conditions[0]

    # Query with optional filtering
    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter,
    )

    chunks = []
    for i in range(len(results["ids"][0])):
        meta = results["metadatas"][0][i]
        chunks.append(
            {
                "content": results["documents"][0][i],
                "source_file": meta.get("source_file", ""),
                "chapter_title": meta.get("chapter_title", ""),
                "section_title": meta.get("section_title", ""),
                "document_title": meta.get("document_title", ""),
                "game_system": meta.get("game_system", ""),
                "content_type": meta.get("content_type", ""),
                "tags": meta.get("tags", ""),
                "distance": results["distances"][0][i],
            }
        )
    return chunks


def search_duckdb(
    query: str,
    limit: int = 5,
    game_system: Optional[str] = None,
    content_type: Optional[str] = None,
) -> list[dict]:
    """
    Keyword search over chunks in documents_tabletop_rules schema.

    Args:
        query: The search query
        limit: Max results to return
        game_system: Filter by game system
        content_type: Filter by content type
    """
    conn = duckdb.connect(DB_PATH, read_only=True)

    # Build WHERE clauses with parameterized queries to prevent SQL injection
    where_clauses = []
    params = []

    # Keyword search using OR logic + relevance ranking by match count
    words = [w for w in query.lower().split() if len(w) > 2]
    if words:
        or_clauses = []
        for w in words:
            or_clauses.append("LOWER(c.content) LIKE ?")
            params.append(f"%{w}%")
        where_clauses.append(f"({' OR '.join(or_clauses)})")

    # Project-specific filters (parameterized)
    if game_system:
        where_clauses.append("f.game_system = ?")
        params.append(game_system)
    if content_type:
        where_clauses.append("f.content_type = ?")
        params.append(content_type)

    where = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # Rank by number of matching keywords (more matches = more relevant)
    match_score_expr = " + ".join(
        [f"CASE WHEN LOWER(c.content) LIKE ? THEN 1 ELSE 0 END" for _ in words]
    ) if words else "0"
    score_params = [f"%{w}%" for w in words]

    sql = f"""
        SELECT c.source_file, c.section_title, c.content,
               f.document_title, f.game_system, f.content_type, f.tags,
               ({match_score_expr}) as match_score,
               c.chapter_title
        FROM documents_tabletop_rules.chunks c
        LEFT JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
        {where}
        ORDER BY match_score DESC
        LIMIT {limit}
    """

    try:
        rows = conn.execute(sql, params + score_params).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    return [
        {
            "content": r[2],
            "source_file": r[0],
            "chapter_title": r[8] or "",
            "section_title": r[1] or "",
            "document_title": r[3] or "",
            "game_system": r[4] or "",
            "content_type": r[5] or "",
            "tags": r[6] or "",
            "distance": 0.0,
        }
        for r in rows
    ]


def retrieve_context(
    query: str,
    n_results: int = 10,
    game_system: Optional[str] = None,
    content_type: Optional[str] = None,
) -> list[dict]:
    """
    Retrieve context using hybrid search (semantic + keyword).

    Args:
        query: The search query
        n_results: Number of results to combine
        game_system: Optional filter by game system
        content_type: Optional filter by content type
    """
    # Over-fetch from each track to ensure we don't miss relevant results
    fetch_count = n_results * 3
    semantic = search_chromadb(
        query, n_results=fetch_count, game_system=game_system, content_type=content_type
    )
    keyword = search_duckdb(
        query, limit=fetch_count, game_system=game_system, content_type=content_type
    )

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
    n_results: int = 10,
    show_sources: bool = True,
    game_system: Optional[str] = None,
    content_type: Optional[str] = None,
) -> str:
    """
    Answer a question using retrieved document context + LLM.

    Args:
        question: The question to answer
        model: LLM model to use (default: llama3:70b)
        n_results: Number of context chunks to retrieve
        show_sources: Include source citations in response
        game_system: Filter results by game system (e.g., "D&D 2e")
        content_type: Filter results by content type (e.g., "rules")
    """
    chunks = retrieve_context(
        question,
        n_results=n_results,
        game_system=game_system,
        content_type=content_type,
    )

    if not chunks:
        return (
            "No relevant documents found. "
            "Please ingest PDFs first: run load_tabletop_rules_docs.py and embed_tabletop_rules.py"
        )

    # Build context with metadata including chapter location
    context_parts = []
    for c in chunks:
        source_info = f"[{c['source_file']}"
        if c.get("chapter_title"):
            source_info += f" | Chapter: {c['chapter_title']}"
        if c.get("section_title"):
            source_info += f" | {c['section_title']}"
        if c.get("game_system"):
            source_info += f" | {c['game_system']}"
        source_info += "]"

        context_parts.append(f"{source_info}\n{c['content']}")

    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""You are a knowledgeable assistant for tabletop RPG and board game rules.
Answer the question based ONLY on the provided context from official rules documents.
If the answer is not in the context, say so clearly.
Cite the source document, section, and game system when possible.

CONTEXT:
{context}

QUESTION: {question}

ANSWER:"""

    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=600,
        )
        response.raise_for_status()
        answer = response.json()["response"]
    except requests.exceptions.RequestException as e:
        return f"Error calling LLM: {e}. Check that Ollama is running on {OLLAMA_URL}"

    if show_sources:
        source_list = []
        for c in chunks:
            title = c.get("document_title") or c.get("source_file", "Unknown")
            source_list.append(f"  - {title} ({c.get('game_system', '')})")

        answer += f"\n\nSources used:\n" + "\n".join(source_list)

    return answer


if __name__ == "__main__":
    import sys

    # Example usage from CLI
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the basic rules?"
    print(ask(question, game_system="D&D 2e"))
