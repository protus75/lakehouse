-- Fail if any table has rows with fewer than 2 columns.
-- A table should have at least 2 columns; single-column rows indicate
-- missing columns or a parsing failure.
with row_widths as (
    select
        table_number,
        table_title,
        row_index,
        json_array_length(cells::json) as col_count
    from {{ source('bronze_tabletop', 'tables_raw') }}
)

select
    table_number,
    table_title,
    row_index,
    col_count
from row_widths
where col_count < 2
