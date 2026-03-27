"""Dagster asset definitions for the lakehouse pipeline.

Asset graph:
    bronze_tabletop → dbt_tabletop → publish_to_iceberg
        → gold_ai_summaries
        → gold_ai_annotations

Bronze, dbt, and publish run in the Dagster daemon (same volumes).
Enrichment runs via docker exec on lakehouse-workspace (GPU + Ollama).
"""
import sys
sys.path.insert(0, "/workspace")

import subprocess
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    Config,
)

DBT_PROJECT_DIR = Path("/workspace/dbt/lakehouse_mvp")


class BronzeConfig(Config):
    force: bool = False


@asset(group_name="bronze", compute_kind="python")
def bronze_tabletop(context: AssetExecutionContext, config: BronzeConfig):
    """Extract PDFs to bronze Iceberg tables via dlt."""
    from dlt.bronze_tabletop_rules import run
    run(force=config.force)
    context.log.info("Bronze extraction complete")


@asset(group_name="bronze", compute_kind="python", deps=[bronze_tabletop])
def toc_review(context: AssetExecutionContext):
    """Review parsed ToC and Marker headings for manual validation.

    Onboarding gate: blocks pipeline if any book has toc_reviewed=false.
    On first run for a new book, this asset FAILS — review the Dagster log
    output, update the book's YAML config, then re-run:
      1. Set toc_reviewed: true
      2. Add toc_corrections for any title/page fixes
      3. Add valid_section_headings for legitimate H2 sub-sections
    """
    from dlt.bronze_tabletop_rules import review_toc, apply_toc_review
    report = review_toc()
    # Apply reviewed YAML files if they exist
    for file_report in report.get("files", []):
        if file_report["status"] == "pass":
            apply_toc_review(file_report["source_file"])
            context.log.info(f"Applied ToC review for {file_report['source_file']}")
    for file_report in report.get("files", []):
        sf = file_report["source_file"]
        status = file_report["status"]
        context.log.info(f"ToC review {sf}: {status}")
        if file_report.get("unrecognized_h1"):
            context.log.warning(f"  Unrecognized H1: {file_report['unrecognized_h1']}")
    if report["status"] == "needs_review":
        needs = [r["source_file"] for r in report["files"] if r["status"] == "needs_review"]
        raise Exception(
            f"ToC review required for: {', '.join(needs)}. "
            f"Review the log output above, then update each book's YAML config "
            f"(toc_reviewed, toc_corrections, valid_section_headings) and re-run."
        )
    context.log.info(f"ToC review passed for all {len(report['files'])} books")


@asset(group_name="bronze", compute_kind="python", deps=[toc_review])
def bronze_ocr_check(context: AssetExecutionContext):
    """Bronze OCR validation: dictionary-based spellcheck of markdown content.

    Checks all words against English dictionary + game terms whitelist.
    No LLM needed — runs in seconds. Results in bronze_tabletop.ocr_issues.
    """
    from pathlib import Path
    from dlt.bronze_tabletop_rules import check_ocr, DOCUMENTS_DIR
    pdfs = sorted(DOCUMENTS_DIR.glob("*.pdf"))
    for f in pdfs:
        context.log.info(f"OCR check: {f.name}")
        check_ocr(f.name)
    context.log.info(f"OCR check complete for {len(pdfs)} books")


@asset(group_name="silver_gold", compute_kind="dbt", deps=[bronze_ocr_check])
def dbt_tabletop(context: AssetExecutionContext):
    """Run dbt build for tabletop models (silver + gold)."""
    result = subprocess.run(
        ["dbt", "build", "--select", "tabletop", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True, text=True, cwd=str(DBT_PROJECT_DIR),
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"dbt build failed: {result.stderr}")
    context.log.info("dbt build complete")


@asset(group_name="silver_gold", compute_kind="python", deps=[dbt_tabletop])
def publish_to_iceberg(context: AssetExecutionContext):
    """Publish dbt silver/gold tables to Iceberg on S3."""
    from dlt.publish_to_iceberg import publish
    publish()
    context.log.info("Published silver/gold to Iceberg")


@asset(group_name="enrichment", compute_kind="ollama", deps=[publish_to_iceberg])
def gold_ai_summaries(context: AssetExecutionContext):
    """AI-generated summaries for gold entries via Ollama. ~45min for 800+ entries."""
    from scripts.tabletop_rules.enrich_summaries import main
    main()
    context.log.info("AI summaries complete")


@asset(group_name="enrichment", compute_kind="ollama", deps=[gold_ai_summaries])
def gold_ai_annotations(context: AssetExecutionContext):
    """AI-generated combat/popular annotations via Ollama. ~25min for 500+ entries.

    Runs after summaries to avoid competing for Ollama VRAM.
    """
    from scripts.tabletop_rules.enrich_annotations import main
    main()
    context.log.info("AI annotations complete")


@asset(group_name="system", compute_kind="python")
def seed_ollama_models(context: AssetExecutionContext):
    """Pull all Ollama models defined in lakehouse.yaml.

    Ollama runs on the Windows host. This asset calls the Ollama API
    from inside the container via host.docker.internal.
    """
    import yaml
    import requests

    config_path = Path("/workspace/config/lakehouse.yaml")
    config = yaml.safe_load(config_path.read_text())
    ollama_config = config.get("models", {}).get("ollama", {})
    url = ollama_config.get("url", "http://host.docker.internal:11434")
    models = ollama_config.get("models", [])

    if not models:
        context.log.warning("No Ollama models configured in lakehouse.yaml")
        return

    for model in models:
        context.log.info(f"Pulling {model}...")
        resp = requests.post(
            f"{url}/api/pull",
            json={"name": model, "stream": False},
            timeout=1800,
        )
        resp.raise_for_status()
        context.log.info(f"  {model}: OK")

    # Verify all models are available
    resp = requests.get(f"{url}/api/tags", timeout=10)
    resp.raise_for_status()
    available = {m["name"] for m in resp.json().get("models", [])}
    for model in models:
        if model not in available:
            raise Exception(f"Model {model} not found after pull")
    context.log.info(f"All {len(models)} Ollama models verified")


@asset(group_name="system", compute_kind="python")
def seed_huggingface_models(context: AssetExecutionContext):
    """Verify HuggingFace models are cached. Fails if any are missing.

    Daemon has TRANSFORMERS_OFFLINE=1 and no sentence-transformers, so
    this just checks the cache directory. To download missing models:
      docker exec lakehouse-workspace python -c "
        from sentence_transformers import SentenceTransformer;
        SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
      "
    """
    import yaml

    config_path = Path("/workspace/config/lakehouse.yaml")
    config = yaml.safe_load(config_path.read_text())
    hf_models = config.get("models", {}).get("huggingface", {}).get("models", [])

    if not hf_models:
        context.log.warning("No HuggingFace models configured in lakehouse.yaml")
        return

    hf_home = Path("/workspace/cache/huggingface")
    missing = []
    for model_name in hf_models:
        # HF caches models at hub/models--<org>--<name>/
        safe_name = model_name.replace("/", "--")
        model_dir = hf_home / "hub" / f"models--{safe_name}"
        if model_dir.exists():
            context.log.info(f"Cache OK: {model_name} -> {model_dir}")
        else:
            missing.append(model_name)
            context.log.error(f"Cache MISSING: {model_name}")

    if missing:
        raise Exception(
            f"HuggingFace cache missing for {len(missing)} models: {', '.join(missing)}\n"
            f"Run on workspace container: docker exec lakehouse-workspace python -c "
            f"\"from sentence_transformers import SentenceTransformer; "
            + "; ".join(f"SentenceTransformer('{m}')" for m in missing)
            + '"'
        )
    context.log.info(f"All {len(hf_models)} HuggingFace models cached")


@asset(group_name="system", compute_kind="python")
def seed_marker_cache(context: AssetExecutionContext):
    """Validate Marker OCR cache exists for all PDFs. Fails if any are missing.

    To generate missing cache, run on the workspace container (GPU):
      docker exec lakehouse-workspace python -c "
        from dlt.bronze_tabletop_rules import extract_marker_markdown, DOCUMENTS_DIR
        [extract_marker_markdown(f, allow_ocr=True) for f in sorted(DOCUMENTS_DIR.glob('*.pdf'))]
      "
    """
    import yaml
    config = yaml.safe_load(Path("/workspace/config/lakehouse.yaml").read_text())
    paths = config.get("paths", {})
    DOCUMENTS_DIR = Path(paths.get("documents", "/workspace/documents/tabletop_rules/raw"))
    MARKER_CACHE_DIR = Path(paths.get("marker_cache", "/workspace/documents/tabletop_rules/processed/marker"))
    pdfs = sorted(DOCUMENTS_DIR.glob("*.pdf"))
    missing = []
    for f in pdfs:
        cache_path = MARKER_CACHE_DIR / f"{f.stem.replace(' ', '_')}.md"
        if cache_path.exists():
            context.log.info(f"Cache OK: {f.name} -> {cache_path.name}")
        else:
            missing.append(f.name)
            context.log.error(f"Cache MISSING: {f.name}")
    if missing:
        raise Exception(
            f"Marker cache missing for {len(missing)} PDFs: {', '.join(missing)}\n"
            f"Run on workspace container (GPU): docker exec lakehouse-workspace python -c "
            f"\"from dlt.bronze_tabletop_rules import extract_marker_markdown, DOCUMENTS_DIR; "
            f"[extract_marker_markdown(f, allow_ocr=True) for f in sorted(DOCUMENTS_DIR.glob('*.pdf'))]\""
        )
    context.log.info(f"All {len(pdfs)} PDF caches validated")


# Jobs
tabletop_full_pipeline = define_asset_job(
    name="tabletop_full_pipeline",
    selection=[
        bronze_tabletop, toc_review, bronze_ocr_check, dbt_tabletop,
        publish_to_iceberg, gold_ai_summaries, gold_ai_annotations,
    ],
)

tabletop_without_enrichment = define_asset_job(
    name="tabletop_without_enrichment",
    selection=[bronze_tabletop, toc_review, bronze_ocr_check, dbt_tabletop, publish_to_iceberg],
)

enrichment_only = define_asset_job(
    name="enrichment_only",
    selection=[gold_ai_summaries, gold_ai_annotations],
)

seed_models = define_asset_job(
    name="seed_models",
    selection=[seed_ollama_models, seed_huggingface_models, seed_marker_cache],
)


defs = Definitions(
    assets=[
        seed_ollama_models, seed_huggingface_models, seed_marker_cache,
        bronze_tabletop, toc_review, bronze_ocr_check, dbt_tabletop,
        publish_to_iceberg, gold_ai_summaries, gold_ai_annotations,
    ],
    jobs=[seed_models, tabletop_full_pipeline, tabletop_without_enrichment, enrichment_only],
)
