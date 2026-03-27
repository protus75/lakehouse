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


@asset(group_name="bronze", compute_kind="ollama", deps=[toc_review])
def bronze_ocr_check(context: AssetExecutionContext):
    """Bronze OCR validation: scan markdown for OCR errors using Ollama (llama3:8b).

    Resumable — skips chunks already checked. Results stored in bronze_tabletop.ocr_issues.
    Confirmed fixes should be added to content_substitutions in the book config.
    """
    from pathlib import Path
    from dlt.bronze_tabletop_rules import check_ocr, DOCUMENTS_DIR
    pdfs = sorted(DOCUMENTS_DIR.glob("*.pdf"))
    for f in pdfs:
        context.log.info(f"OCR check: {f.name}")
        check_ocr(f.name, resume=True)
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
def seed_marker_cache(context: AssetExecutionContext):
    """Validate Marker OCR cache exists for all PDFs. Fails if any are missing.

    To generate missing cache, run on the workspace container (GPU):
      docker exec lakehouse-workspace python -c "
        from dlt.bronze_tabletop_rules import extract_marker_markdown, DOCUMENTS_DIR
        [extract_marker_markdown(f, allow_ocr=True) for f in sorted(DOCUMENTS_DIR.glob('*.pdf'))]
      "
    """
    from dlt.bronze_tabletop_rules import MARKER_CACHE_DIR, DOCUMENTS_DIR
    pdfs = sorted(DOCUMENTS_DIR.glob("*.pdf"))
    missing = []
    for f in pdfs:
        cache_path = MARKER_CACHE_DIR / f"{f.stem}.md"
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
    selection=[seed_marker_cache],
)


defs = Definitions(
    assets=[
        seed_marker_cache,
        bronze_tabletop, toc_review, bronze_ocr_check, dbt_tabletop,
        publish_to_iceberg, gold_ai_summaries, gold_ai_annotations,
    ],
    jobs=[seed_models, tabletop_full_pipeline, tabletop_without_enrichment, enrichment_only],
)
