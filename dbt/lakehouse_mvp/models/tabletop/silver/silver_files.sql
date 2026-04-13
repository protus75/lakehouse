-- Silver file-level metadata aggregated from entries

select
    e.source_file,
    f.pdf_size_bytes,
    f.total_pages,
    count(*) as total_entries,
    count(case when e.entry_title is not null then 1 end) as named_entries,
    sum(e.char_count) as total_chars,
    current_timestamp as processed_at
-- silver_entries is a Dagster asset (not a dbt model). Read from the
-- iceberg view created by create_bronze_views macro at dbt run start.
from silver_tabletop.silver_entries e
join {{ source('bronze_tabletop', 'files') }} f
    on e.source_file = f.source_file
group by e.source_file, f.pdf_size_bytes, f.total_pages
