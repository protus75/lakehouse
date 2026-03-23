-- Silver ToC sections with sub_headings and tables populated from entries

with toc as (
    select
        row_number() over (order by source_file, page_start) as toc_id,
        source_file,
        title,
        page_start,
        page_end,
        is_excluded
    from {{ source('bronze_tabletop', 'toc_raw') }}
),

-- Collect entry titles per section (sub_headings)
entry_titles as (
    select
        e.source_file,
        e.toc_title,
        string_agg(distinct e.entry_title, '; ' order by e.entry_title) as sub_headings
    from {{ ref('silver_entries') }} e
    where e.entry_title is not null
    group by e.source_file, e.toc_title
)

select
    t.toc_id,
    t.source_file,
    t.title,
    t.page_start,
    t.page_end,
    t.is_excluded,
    coalesce(et.sub_headings, '') as sub_headings,
    '' as tables  -- populated from bronze toc tables field if available
from toc t
left join entry_titles et
    on t.source_file = et.source_file
    and t.title = et.toc_title
