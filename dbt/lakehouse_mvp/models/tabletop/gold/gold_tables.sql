-- Gold tables: query-ready table data for the browser app.
-- Pass-through from silver — no AI augmentation yet.

select
    source_file,
    table_number,
    table_title,
    toc_title,
    toc_id,
    parent_title,
    sort_order,
    format,
    row_index,
    cells
from {{ ref('silver_tables') }}
