Write-Host "=== Step 1/4: Rebuilding container ===" -ForegroundColor Cyan
Set-Location D:\source\lakehouse\lakehouse\docker
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Write-Host "Build failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 2/4: Re-ingesting PDFs ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', content_type='rules')"
if ($LASTEXITCODE -ne 0) { Write-Host "Ingestion failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 3/4: Deleting old ChromaDB collection ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import chromadb
from chromadb.config import Settings
client = chromadb.PersistentClient(path='/workspace/chroma_db', settings=Settings(anonymized_telemetry=False))
try:
    client.delete_collection('tabletop_rules_chunks')
    print('Collection deleted.')
except Exception:
    print('No existing collection to delete, skipping.')
"
if ($LASTEXITCODE -ne 0) { Write-Host "Collection delete failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 4/4: Re-embedding chunks ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from rag.embed_tabletop_rules import embed_all; embed_all()"
if ($LASTEXITCODE -ne 0) { Write-Host "Embedding failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Done! Testing with sample query ===" -ForegroundColor Green
docker exec lakehouse-workspace python -c "from rag.query_tabletop_rules import ask; print(ask('How many rounds to cast priest bless spell?', game_system='D&D 2e'))"
