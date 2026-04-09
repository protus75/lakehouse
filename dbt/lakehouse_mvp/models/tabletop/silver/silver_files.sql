-- Silver file-level metadata aggregated from entries
--
-- Phase 2a proof model: materializes directly to iceberg via the custom
-- iceberg plugin (dlt/lib/dbt_iceberg_plugin.py). Once verified end-to-end,
-- Phase 2c rolls this config up to dbt_project.yml as the silver default
-- and removes this per-model block.
{{ config(
    materialized='external',
    plugin='iceberg',
    location='/scratch/dbt/silver_files.parquet'
) }}

select
    e.source_file,
    f.pdf_size_bytes,
    f.total_pages,
    count(*) as total_entries,
    count(case when e.entry_title is not null then 1 end) as named_entries,
    sum(e.char_count) as total_chars,
    current_timestamp as processed_at
from {{ ref('silver_entries') }} e
join {{ source('bronze_tabletop', 'files') }} f
    on e.source_file = f.source_file
group by e.source_file, f.pdf_size_bytes, f.total_pages
