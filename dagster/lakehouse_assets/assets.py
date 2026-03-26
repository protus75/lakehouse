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


@asset(group_name="silver_gold", compute_kind="dbt", deps=[bronze_tabletop])
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


# Jobs
tabletop_full_pipeline = define_asset_job(
    name="tabletop_full_pipeline",
    selection=[
        bronze_tabletop, dbt_tabletop, publish_to_iceberg,
        gold_ai_summaries, gold_ai_annotations,
    ],
)

tabletop_without_enrichment = define_asset_job(
    name="tabletop_without_enrichment",
    selection=[bronze_tabletop, dbt_tabletop, publish_to_iceberg],
)

enrichment_only = define_asset_job(
    name="enrichment_only",
    selection=[gold_ai_summaries, gold_ai_annotations],
)


defs = Definitions(
    assets=[
        bronze_tabletop, dbt_tabletop, publish_to_iceberg,
        gold_ai_summaries, gold_ai_annotations,
    ],
    jobs=[tabletop_full_pipeline, tabletop_without_enrichment, enrichment_only],
)
