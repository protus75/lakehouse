# Tabletop Rules Project

PDF storage and RAG-enabled search for tabletop RPG and board game rules.

## Directory Structure

```
tabletop_rules/
├── raw/           ← Place your PDF files here
├── processed/     ← Reserved for preprocessing
└── README.md      ← This file
```

## Quick Start

### 1. Add PDFs
Copy your board game or RPG rule PDFs to `raw/` folder.

Examples:
- `D&D_5e_Players_Handbook.pdf`
- `Pathfinder_2e_Core_Rulebook.pdf`
- `Curse_of_Strahd_Campaign.pdf`

### 2. Ingest (In Jupyter)
```python
# Parse PDFs and create chunks
from dlt.load_tabletop_rules_docs import run
run(game_system="D&D 5e", content_type="rules")
```

### 3. Embed
```python
# Generate embeddings for semantic search
from rag.embed_tabletop_rules import embed_all
embed_all()
```

### 4. Query
```python
# Ask questions about the rules
from rag.query_tabletop_rules import ask

answer = ask("What are the rules for attacking?")
print(answer)
```

## Metadata You Can Track

When ingesting PDFs, you can specify:

| Field | Example | Notes |
|-------|---------|-------|
| `game_system` | "D&D 5e" | D&D 5e, Pathfinder 2e, Warhammer 40K, etc. |
| `content_type` | "rules" | rules, module, campaign, supplement |
| `tags` | "combat,spells" | Comma-separated categories |
| `rules_version` | "2024" | Version/edition identifier |

## Advanced Queries

### Filter by Game System
```python
answer = ask("How do you resolve attacks?", game_system="D&D 5e")
```

### Filter by Content Type
```python
# Only search campaign materials
answer = ask("What happens in Act 1?", content_type="campaign")
```

### Keyword Search Only
```python
from rag.query_tabletop_rules import search_duckdb
results = search_duckdb("concentration", game_system="D&D 5e")
```

### Semantic Search Only
```python
from rag.query_tabletop_rules import search_chromadb
results = search_chromadb("spell duration", n_results=10)
```

## Documentation

Full setup guide and examples:
```bash
# Print guide
python rag/setup_tabletop_rules.py

# Check status
python rag/setup_tabletop_rules.py status
```

## Database Schema

**`documents_tabletop_rules.chunks`** - Text chunks
- `chunk_id`: Unique ID
- `source_file`: PDF filename
- `section_title`: Heading from PDF
- `content`: Actual text content
- `char_count`: Length
- `parsed_at`: Parse timestamp

**`documents_tabletop_rules.files`** - Document metadata
- `source_file`: PDF filename
- `document_title`: Official title
- `game_system`: What system this is for
- `content_type`: Type of content
- `tags`: Keywords
- `rules_version`: Edition/version
- `total_chunks`: Number of chunks
- `total_chars`: Total characters
- `parsed_at`: Ingest timestamp

## Vector Store

- **Collection**: `tabletop_rules_chunks` in ChromaDB
- **Embedding Model**: all-MiniLM-L6-v2
- **Search Method**: Cosine similarity + hybrid keyword matching

## LLM

- **Service**: Ollama (self-hosted)
- **Model**: llama3:70b
- **URL**: http://host.docker.internal:11434
- **Timeout**: 120 seconds

## Troubleshooting

### PDFs not ingesting
- Check files are in `raw/` folder
- Ensure PDFs are readable (not corrupted)
- Check Docling library is installed

### Embeddings not working
- Verify ChromaDB path exists
- Re-run embedding after new ingestion

### LLM timeouts
- Check Ollama is running
- Increase timeout in `query_tabletop_rules.py`

### Poor search results
- Ensure you have 3+ PDFs ingested
- Try keyword search (`search_duckdb`) instead of semantic
- Check metadata filters (game_system, content_type)

## Related Files

- **Ingestion**: `dlt/load_tabletop_rules_docs.py`
- **Embedding**: `rag/embed_tabletop_rules.py`
- **Querying**: `rag/query_tabletop_rules.py`
- **Setup & Docs**: `rag/setup_tabletop_rules.py`
