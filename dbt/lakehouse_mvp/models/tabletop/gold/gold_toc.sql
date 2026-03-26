-- Gold ToC: denormalized for query layer with hierarchy

select
    toc_id,
    source_file,
    title,
    page_start,
    page_end,
    sort_order,
    depth,
    is_chapter,
    is_table,
    is_excluded,
    parent_title,
    sub_headings,
    tables
from {{ ref('silver_toc_sections') }}
