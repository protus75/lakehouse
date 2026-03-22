"""
Two-stage query engine for tabletop rule book RAG.

Stage 1: ToC Routing — LLM picks the most relevant ToC section(s) for the question
Stage 2: Section Search — semantic + keyword search within those sections only
Stage 3: Answer Generation — LLM answers from the retrieved context
"""

import re
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
OLLAMA_URL = "http://host.docker.internal:11434"
DEFAULT_MODEL = "llama3:70b"


# ── Stage 1: ToC Routing ─────────────────────────────────────────

def get_toc(source_file: str | None = None) -> list[dict]:
    """Load ToC entries from DuckDB including sub-headings and tables."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    where = "WHERE NOT is_excluded"
    params = []
    if source_file:
        where += " AND source_file = ?"
        params.append(source_file)

    rows = conn.execute(f"""
        SELECT toc_id, title, page_start, page_end, sub_headings, tables
        FROM documents_tabletop_rules.toc
        {where}
        ORDER BY page_start
    """, params).fetchall()
    conn.close()

    return [{
        "toc_id": r[0], "title": r[1], "page_start": r[2], "page_end": r[3],
        "sub_headings": r[4] or "", "tables": r[5] or "",
    } for r in rows]


def _build_toc_prompt_list(toc: list[dict]) -> str:
    """Build the numbered ToC list for LLM prompts."""
    lines = []
    for i, e in enumerate(toc):
        line = f"{i+1}. {e['title']} (pages {e['page_start']}-{e['page_end']})"
        if e.get("sub_headings"):
            subs = e["sub_headings"].split("; ")[:15]
            line += f"\n   Sub-sections: {', '.join(subs)}"
        if e.get("tables"):
            line += f"\n   Tables: {e['tables']}"
        lines.append(line)
    return "\n".join(lines)


def route_to_toc(
    question: str,
    toc: list[dict],
    model: str = DEFAULT_MODEL,
) -> list[dict]:
    """Ask the LLM which ToC section(s) contain the answer.
    Handles multi-entity queries (e.g. "summarize Bless, Sanctuary and Aid")
    by identifying each entity and its section separately.
    Returns list of {entity, toc_entry} dicts."""
    toc_list = _build_toc_prompt_list(toc)

    prompt = f"""You are a rules index lookup tool for a tabletop RPG rule book.
Your job is to identify which TABLE OF CONTENTS section(s) contain the answer.

RULES:
- If the question asks about MULTIPLE named entries (spells, abilities, items, etc.), list EACH entry and its section separately — they may be in different sections
- Look at sub-sections and tables listed under each section to find where specific entries are
- Pick the section whose CONTENT contains the entry, not just a section with a similar title
- For a single topic question, just return one LOOKUP line

TABLE OF CONTENTS:
{toc_list}

QUESTION: {question}

Format your response as one or more LOOKUP lines, EXACTLY like this:
LOOKUP: [entity name or topic] -> [section number]

Examples:
LOOKUP: Bless spell -> 18
LOOKUP: THAC0 table -> 9
LOOKUP: Strength ability score -> 1"""

    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=300,
        )
        response.raise_for_status()
        result = response.json()["response"]
    except Exception as e:
        print(f"ToC routing failed: {e}")
        return []

    return _parse_routing_response(result, toc, question)


def _parse_routing_response(response: str, toc: list[dict], question: str) -> list[dict]:
    """Parse LOOKUP lines from the LLM response.
    Multiple entities can point to the same section — that's normal."""
    lookups = []

    for line in response.split("\n"):
        m = re.search(r"LOOKUP\s*:\s*(.+?)\s*->\s*(\d+)", line)
        if m:
            entity = m.group(1).strip()
            idx = int(m.group(2)) - 1
            if 0 <= idx < len(toc):
                lookups.append({"entity": entity, "toc_entry": toc[idx]})

    return lookups


# ── Stage 2: Section Search ──────────────────────────────────────

def search_section(
    query: str,
    toc_ids: list[int],
    n_results: int = 10,
) -> list[dict]:
    """Search within specific ToC sections using semantic + keyword search."""
    semantic = _search_chromadb(query, toc_ids, n_results)
    keyword = _search_duckdb(query, toc_ids, n_results)

    # Deduplicate, semantic first
    seen = set()
    combined = []
    for chunk in semantic + keyword:
        key = chunk["content"][:200]
        if key not in seen:
            seen.add(key)
            combined.append(chunk)

    # Rerank within section
    combined = _rerank(query, combined)
    return combined[:n_results]


def _search_chromadb(query: str, toc_ids: list[int], n_results: int) -> list[dict]:
    """Semantic search filtered by toc_id."""
    client = chromadb.PersistentClient(
        path=CHROMA_PATH, settings=Settings(anonymized_telemetry=False),
    )
    embedding_fn = SentenceTransformerEmbeddingFunction(model_name=EMBEDDING_MODEL)

    try:
        collection = client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)
    except Exception:
        return []

    # Build toc_id filter
    if len(toc_ids) == 1:
        where_filter = {"toc_id": {"$eq": str(toc_ids[0])}}
    else:
        where_filter = {"$or": [{"toc_id": {"$eq": str(tid)}} for tid in toc_ids]}

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter,
    )

    chunks = []
    for i in range(len(results["ids"][0])):
        meta = results["metadatas"][0][i]
        chunks.append({
            "content": results["documents"][0][i],
            "toc_id": meta.get("toc_id", ""),
            "toc_title": meta.get("toc_title", ""),
            "section_title": meta.get("section_title", ""),
            "entry_title": meta.get("entry_title", ""),
            "page_numbers": meta.get("page_numbers", ""),
            "source_file": meta.get("source_file", ""),
            "distance": results["distances"][0][i],
        })
    return chunks


def _search_duckdb(query: str, toc_ids: list[int], n_results: int) -> list[dict]:
    """Keyword search filtered by toc_id."""
    conn = duckdb.connect(DB_PATH, read_only=True)

    words = [w for w in query.lower().split() if len(w) > 2]
    if not words:
        conn.close()
        return []

    # OR keyword matching with score
    or_clauses = " OR ".join(["LOWER(c.content) LIKE ?" for _ in words])
    score_expr = " + ".join(
        ["CASE WHEN LOWER(c.content) LIKE ? THEN 1 ELSE 0 END" for _ in words]
    )
    params = [f"%{w}%" for w in words]
    score_params = [f"%{w}%" for w in words]

    toc_placeholders = ",".join(["?" for _ in toc_ids])

    sql = f"""
        SELECT c.source_file, c.toc_id, c.section_title, c.entry_title,
               c.content, c.page_numbers, t.title as toc_title,
               ({score_expr}) as match_score
        FROM documents_tabletop_rules.chunks c
        LEFT JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
        WHERE c.toc_id IN ({toc_placeholders})
        AND ({or_clauses})
        ORDER BY match_score DESC
        LIMIT {n_results}
    """

    try:
        rows = conn.execute(sql, toc_ids + params + score_params).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    return [{
        "source_file": r[0],
        "toc_id": str(r[1] or ""),
        "section_title": r[2] or "",
        "entry_title": r[3] or "",
        "content": r[4],
        "page_numbers": r[5] or "",
        "toc_title": r[6] or "",
        "distance": 0.0,
    } for r in rows]


def _rerank(query: str, chunks: list[dict]) -> list[dict]:
    """Rerank by entry title match, stat block presence, and content completeness."""
    query_lower = query.lower()
    query_words = [w for w in query_lower.split() if len(w) > 2]

    scored = []
    for chunk in chunks:
        score = 0.0
        content_lower = chunk["content"].lower()
        entry = (chunk.get("entry_title") or "").lower()

        # Entry title matches query words
        title_match = False
        for w in query_words:
            if w in entry:
                score += 3.0
                title_match = True

        # Stat block fields present
        stat_count = 0
        for field in ["casting time:", "range:", "duration:", "components:",
                      "saving throw:", "area of effect:", "sphere:", "school:"]:
            if field in content_lower:
                stat_count += 1
                score += 0.5

        # Bonus: chunk has both title match AND stat fields (the complete entry)
        if title_match and stat_count >= 3:
            score += 5.0

        # Prefer longer chunks (more complete content)
        score += min(len(chunk["content"]) / 500, 2.0)

        # Content density
        if query_words:
            hits = sum(1 for w in query_words if w in content_lower)
            score += hits * 0.5

        scored.append((score, chunk))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored]


# ── Stage 3: Answer Generation ───────────────────────────────────

def ask(
    question: str,
    model: str = DEFAULT_MODEL,
    n_results_per_entity: int = 5,
    game_system: Optional[str] = None,
) -> str:
    """Answer a question using two-stage ToC-routed retrieval.
    Handles multi-entity queries by searching each entity in its own ToC section."""

    # Stage 1: Route — identify entities and their ToC sections
    toc = get_toc()
    if not toc:
        return "No ToC found. Please ingest a PDF first."

    lookups = route_to_toc(question, toc, model=model)
    if not lookups:
        return "Could not identify relevant sections for this question."

    for lk in lookups:
        print(f"  LOOKUP: '{lk['entity']}' -> {lk['toc_entry']['title']}")

    # Stage 2: Search each entity in its routed section
    all_chunks = []
    for lk in lookups:
        toc_entry = lk["toc_entry"]
        entity = lk["entity"]

        # Search for this specific entity within its section
        chunks = search_section(entity, [toc_entry["toc_id"]], n_results=n_results_per_entity)
        all_chunks.extend(chunks)

    if not all_chunks:
        return "No relevant content found in the identified sections."

    # Deduplicate across entities
    seen = set()
    deduped = []
    for c in all_chunks:
        key = c["content"][:200]
        if key not in seen:
            seen.add(key)
            deduped.append(c)

    # Stage 3: Generate answer
    context_parts = []
    for c in deduped:
        source = f"[{c.get('toc_title', '')}]"
        if c.get("entry_title"):
            source += f" Entry: {c['entry_title']}"
        context_parts.append(f"{source}\n{c['content']}")

    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""You are a knowledgeable assistant for tabletop RPG rules.
Answer the question based ONLY on the provided context from official rules documents.
If the answer is not in the context, say so clearly.
Cite the source section name only. Do NOT include page numbers in your answer.

When summarizing spells, abilities, or similar entries:
- Use the EXACT SAME fields in the EXACT SAME order for EVERY spell, no exceptions:
  School, Sphere, Reversible, Range, Components, Duration, Casting Time, Area of Effect, Saving Throw
- School: list as "School: Necromancy, Conjuration" not "(Necromancy, Conjuration)"
- Sphere: same format "Sphere: Necromantic"
- Reversible: always report as "Reversible: Yes" or "Reversible: No" — if not mentioned, "Reversible: No"
- Components: always list as "Components: V, S, M" or similar — if not in context, "Components: N/A"
- Saving Throw: if none, "Saving Throw: None"
- For spells with a saving throw, always describe BOTH the pass and fail outcomes in the description
- Include the FULL description text — do not abbreviate, truncate, or summarize the spell description
- Use markdown formatting but do NOT shorten any content to fit formatting

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
    except Exception as e:
        return f"LLM error: {e}"

    source_titles = list(dict.fromkeys(lk["toc_entry"]["title"] for lk in lookups))
    answer += "\n\nSources: " + " | ".join(source_titles)
    return answer


if __name__ == "__main__":
    import sys
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the basic rules?"
    print(ask(question))
