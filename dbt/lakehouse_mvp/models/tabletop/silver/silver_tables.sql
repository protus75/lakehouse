-- Silver tables: cleaned table data from bronze tables_raw.
-- Strips HTML tags from cells, trims whitespace, joins to silver_toc_sections.

select
    r.source_file,
    r.table_number,
    r.table_title,
    r.toc_title,
    t.toc_id,
    t.parent_title,
    t.sort_order,
    r.format,
    r.row_index,
    -- Clean cells: strip HTML tags, trim whitespace
    regexp_replace(
        regexp_replace(r.cells, '<[^>]+>', '', 'g'),
        '\s+', ' ', 'g'
    ) as cells
from {{ source('bronze_tabletop', 'tables_raw') }} r
left join {{ ref('silver_toc_sections') }} t
    on r.toc_title = t.title
    and r.source_file = t.source_file
    and t.is_table = true
order by r.source_file, r.table_number, r.row_index
