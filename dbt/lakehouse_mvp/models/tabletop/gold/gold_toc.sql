-- Gold ToC: denormalized for query layer

select
    toc_id,
    source_file,
    title,
    page_start,
    page_end,
    is_excluded,
    sub_headings,
    tables
from {{ ref('silver_toc_sections') }}
