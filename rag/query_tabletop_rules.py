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
                "entry_title": meta.get("entry_title", ""),
                "document_title": meta.get("document_title", ""),
                "game_system": meta.get("game_system", ""),
                "content_type": meta.get("content_type", ""),
                "chunk_type": meta.get("chunk_type", "content"),
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
               c.chapter_title, c.entry_title, c.chunk_type
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
            "entry_title": r[9] or "",
            "document_title": r[3] or "",
            "game_system": r[4] or "",
            "content_type": r[5] or "",
            "chunk_type": r[10] or "content",
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

    # Rerank: boost chunks that match the query type
    combined = _rerank_results(query, combined)

    return combined[:n_results]


# Fields that indicate a chunk is a spell/ability stat block
_STAT_BLOCK_INDICATORS = [
    "casting time:", "components:", "duration:", "range:",
    "area of effect:", "saving throw:", "sphere:", "school:",
    "power score:", "psp cost:",
]

# Words in a query that suggest the user is asking about a spell/ability
_SPELL_QUERY_WORDS = {
    "spell", "spells", "cast", "casting", "cantrip",
    "level", "wizard", "priest", "cleric", "mage",
    "school", "sphere", "component", "duration", "range",
    "saving throw", "area of effect",
}


def _rerank_results(query: str, chunks: list[dict]) -> list[dict]:
    """Rerank results with multiple relevance signals."""
    query_lower = query.lower()
    query_words = set(query_lower.split())
    # Words longer than 2 chars for matching
    query_terms = [w for w in query_words if len(w) > 2]

    is_spell_query = bool(query_words & _SPELL_QUERY_WORDS)
    is_list_query = any(w in query_lower for w in ["list", "all", "every", "which", "what are the"])

    # Extract entity names from query — words that aren't common query words
    entity_names = [w for w in query_terms if w not in _SPELL_QUERY_WORDS
                    and w not in {"how", "many", "what", "does", "the", "for", "can", "you"}]

    # Check for class/type keywords in query
    query_class_words = {w for w in query_words if w in {
        "priest", "cleric", "wizard", "mage", "druid", "paladin", "ranger",
        "bard", "thief", "rogue", "fighter", "warrior",
    }}

    scored = []
    for chunk in chunks:
        score = 0.0
        content_lower = chunk["content"].lower()
        entry_title = (chunk.get("entry_title") or "").lower()
        chapter_lower = (chunk.get("chapter_title") or "").lower()
        section_lower = (chunk.get("section_title") or "").lower()
        chunk_type = chunk.get("chunk_type", "content")

        # 1. Entry title match — strongest signal (+5)
        for name in entity_names:
            if name in entry_title:
                score += 5.0
                break

        # 2. Stat block fields boost for spell queries (+0.5 per field, max +4)
        if is_spell_query:
            stat_field_count = sum(1 for f in _STAT_BLOCK_INDICATORS if f in content_lower)
            score += min(stat_field_count * 0.5, 4.0)

        # 3. Chapter/section matches query class words (+2)
        if query_class_words:
            for cw in query_class_words:
                if cw in chapter_lower:
                    score += 2.0
                    break
                if cw in section_lower:
                    score += 1.5
                    break

        # 4. Content density — query terms per 100 chars (+0-2)
        if query_terms and content_lower:
            hits = sum(1 for t in query_terms if t in content_lower)
            density = hits / max(len(content_lower) / 100, 1)
            score += min(density, 2.0)

        # 5. Exact phrase match — consecutive query words in content (+2)
        if len(query_terms) >= 2:
            for i in range(len(query_terms) - 1):
                phrase = f"{query_terms[i]} {query_terms[i+1]}"
                if phrase in content_lower:
                    score += 2.0
                    break

        # 6. Summary chunks get small boost for direct questions (+0.5)
        if chunk_type == "summary" and not is_list_query:
            score += 0.5

        # 7. Cross-reference chunks — boost for list queries, penalize for direct
        if chunk_type == "cross_reference":
            if is_list_query:
                score += 2.0
            else:
                score -= 1.0

        # 8. Penalize chunks with no chapter (orphaned/index content)
        if not chunk.get("chapter_title"):
            score -= 3.0

        # 9. Penalize chapter mismatch — if query says "priest" but chunk is in wizard chapter
        if query_class_words and chapter_lower:
            chapter_has_match = any(cw in chapter_lower for cw in query_class_words)
            chapter_has_conflict = any(
                conflict in chapter_lower
                for cw in query_class_words
                for conflict in _CLASS_CONFLICTS.get(cw, [])
            )
            if chapter_has_conflict and not chapter_has_match:
                score -= 2.0

        scored.append((score, chunk))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [chunk for _, chunk in scored]


# Class word conflicts — if query says "priest", penalize "wizard" chapters
_CLASS_CONFLICTS = {
    "priest": ["wizard", "mage"],
    "cleric": ["wizard", "mage"],
    "druid": ["wizard", "mage"],
    "wizard": ["priest", "cleric", "druid"],
    "mage": ["priest", "cleric", "druid"],
}


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
