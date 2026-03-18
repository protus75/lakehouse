
Write-Host "=== Step 1/6: Rebuilding container ===" -ForegroundColor Cyan
Set-Location D:\source\lakehouse\lakehouse\docker
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Write-Host "Build failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 2/6: Pulling VLM model for Pass 2 ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import requests
try:
    r = requests.post('http://host.docker.internal:11434/api/pull', json={'name': 'minicpm-v', 'stream': False}, timeout=600)
    print(f'Model pull: {r.status_code}')
except Exception as e:
    print(f'VLM pull skipped (Ollama may not be running): {e}')
"

Write-Host "`n=== Step 3/6: Dropping old DuckDB tables (schema changed) ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import duckdb
conn = duckdb.connect('/workspace/db/lakehouse.duckdb')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.chunks')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.files')
print('Old tables dropped.')
conn.close()
"

Write-Host "`n=== Step 4/6: Re-ingesting PDFs (Marker + VLM) ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', content_type='rules')"
if ($LASTEXITCODE -ne 0) { Write-Host "Ingestion failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 5/6: Deleting old ChromaDB collection ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import chromadb
from chromadb.config import Settings
client = chromadb.PersistentClient(path='/workspace/chroma_db', settings=Settings(anonymized_telemetry=False))
try:
    client.delete_collection('tabletop_rules_chunks')
    print('Collection deleted.')
except Exception:
    print('No existing collection, skipping.')
"

Write-Host "`n=== Step 6/6: Re-embedding chunks ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from rag.embed_tabletop_rules import embed_all; embed_all()"
if ($LASTEXITCODE -ne 0) { Write-Host "Embedding failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Done! Testing with sample query ===" -ForegroundColor Green
docker exec lakehouse-workspace python -c "from rag.query_tabletop_rules import ask; print(ask('How many rounds to cast priest bless spell?', game_system='D&D 2e'))"
