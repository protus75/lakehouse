"""FastAPI service exposing RAG queries over HTTP."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from query import ask, retrieve_context, DEFAULT_MODEL

app = FastAPI(title="Lakehouse RAG API", version="1.0.0")


class QueryRequest(BaseModel):
    question: str
    model: str = DEFAULT_MODEL
    n_results: int = 5
    show_sources: bool = True


class QueryResponse(BaseModel):
    answer: str
    question: str
    model: str


class SearchRequest(BaseModel):
    query: str
    n_results: int = 5


class ChunkResult(BaseModel):
    content: str
    source_file: str
    section_title: str
    distance: float


@app.post("/ask", response_model=QueryResponse)
def ask_question(req: QueryRequest):
    try:
        answer = ask(
            req.question,
            model=req.model,
            n_results=req.n_results,
            show_sources=req.show_sources,
        )
        return QueryResponse(answer=answer, question=req.question, model=req.model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/search", response_model=list[ChunkResult])
def search_documents(req: SearchRequest):
    try:
        chunks = retrieve_context(req.query, n_results=req.n_results)
        return [ChunkResult(**c) for c in chunks]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "ok"}
