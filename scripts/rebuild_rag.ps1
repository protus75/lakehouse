param(
    [switch]$NoBuild,
    [switch]$NoEnrich
)

$enrich = if ($NoEnrich) { "False" } else { "True" }

if (-not $NoBuild) {
    Write-Host "=== Step 1: Rebuilding container ===" -ForegroundColor Cyan
    Set-Location D:\source\lakehouse\lakehouse\docker
    docker compose up -d --build
    if ($LASTEXITCODE -ne 0) { Write-Host "Build failed" -ForegroundColor Red; exit 1 }
} else {
    Write-Host "=== Step 1: Skipping build (--NoBuild) ===" -ForegroundColor Yellow
}

Write-Host "`n=== Step 2: Dropping old DuckDB tables ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import duckdb
conn = duckdb.connect('/workspace/db/lakehouse.duckdb')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.chunks')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.files')
print('Tables dropped.')
conn.close()
"

Write-Host "`n=== Step 3: Re-ingesting PDFs (enrich=$enrich) ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', content_type='rules', enrich=$enrich)"
if ($LASTEXITCODE -ne 0) { Write-Host "Ingestion failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 4: Deleting old ChromaDB collection ===" -ForegroundColor Cyan
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

Write-Host "`n=== Step 5: Re-embedding chunks ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from rag.embed_tabletop_rules import embed_all; embed_all()"
if ($LASTEXITCODE -ne 0) { Write-Host "Embedding failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Done! Testing with sample query ===" -ForegroundColor Green
docker exec lakehouse-workspace python -c "from rag.query_tabletop_rules import ask; print(ask('How many rounds to cast priest bless spell?', game_system='D&D 2e'))"
