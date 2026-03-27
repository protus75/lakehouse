-- Fail if any parsed table has zero data rows (only header or completely empty).
-- A table with a title but no rows indicates a parsing failure.
select
    table_number,
    table_title,
    count(*) as row_count
from {{ source('bronze_tabletop', 'tables_raw') }}
group by table_number, table_title
having count(*) < 2
