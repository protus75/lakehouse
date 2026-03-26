-- Silver ToC sections with hierarchy from bronze, sub_headings from entries

with latest_run as (
    select max(run_id) as run_id
    from {{ source('bronze_tabletop', 'toc_raw') }}
),

toc as (
    select
        row_number() over (order by source_file, sort_order) as toc_id,
        source_file,
        title,
        page_start,
        page_end,
        sort_order,
        coalesce(depth, 0) as depth,
        coalesce(is_chapter, true) as is_chapter,
        coalesce(is_table, false) as is_table,
        is_excluded,
        parent_title
    from {{ source('bronze_tabletop', 'toc_raw') }}
    where run_id = (select run_id from latest_run)
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
    t.sort_order,
    t.depth,
    t.is_chapter,
    t.is_table,
    t.is_excluded,
    t.parent_title,
    coalesce(et.sub_headings, '') as sub_headings,
    '' as tables
from toc t
left join entry_titles et
    on t.source_file = et.source_file
    and t.title = et.toc_title
