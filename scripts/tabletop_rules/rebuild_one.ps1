param(
    [string]$Book = "DnD2e Handbook Player.pdf"
)

Write-Host "=== Rebuilding single book: $Book ===" -ForegroundColor Cyan

Write-Host "`n=== Step 1: Dropping old tables ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "
import duckdb
conn = duckdb.connect('/workspace/db/lakehouse.duckdb')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.chunks')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.toc')
conn.execute('DROP TABLE IF EXISTS documents_tabletop_rules.files')
print('Tables dropped.')
conn.close()
"

Write-Host "`n=== Step 2: Ingesting $Book ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from dlt.load_tabletop_rules_docs import parse_pdf; from pathlib import Path; parse_pdf(Path('/workspace/documents/tabletop_rules/raw/$Book'), game_system='D&D 2e', content_type='rules')"
if ($LASTEXITCODE -ne 0) { Write-Host "Ingestion failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 3: Deleting old ChromaDB collection ===" -ForegroundColor Cyan
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

Write-Host "`n=== Step 4: Embedding chunks ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from rag.embed_tabletop_rules import embed_all; embed_all()"
if ($LASTEXITCODE -ne 0) { Write-Host "Embedding failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Step 5: Validating ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python /workspace/scripts/tabletop_rules/validate_spells.py

Write-Host "`n=== Step 6: Exporting priest spells ===" -ForegroundColor Cyan
docker exec lakehouse-workspace python -c "from rag.export import export_markdown; export_markdown('priest_spells')"

Write-Host "`n=== Step 7: Testing query ===" -ForegroundColor Green
docker exec lakehouse-workspace python -c "from rag.query_tabletop_rules import ask; print(ask('How many rounds to cast priest bless spell?'))"
