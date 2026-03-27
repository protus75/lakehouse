-- Fail if any table cell contains a long string with no whitespace.
-- This indicates smushed columns (e.g. "BardLevel1234567891011121314151617181920").
-- Threshold: any cell value over 25 chars with no spaces is likely smushed.
with cell_values as (
    select
        table_number,
        table_title,
        row_index,
        unnest(from_json(cells, '["VARCHAR"]')) as cell
    from {{ source('bronze_tabletop', 'tables_raw') }}
)

select
    table_number,
    table_title,
    row_index,
    cell
from cell_values
where length(cell) > 25
  and cell not like '% %'
  and cell != ''
