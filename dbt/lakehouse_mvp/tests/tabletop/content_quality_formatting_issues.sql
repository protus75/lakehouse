-- Fail if entry content has known formatting problems:
-- 1. Content ends with the entry title repeated (duplicate heading at end)
-- 2. Content contains parenthetical chapter references from ToC chapters
-- 3. Content contains residual HTML tags that cleanup missed
with title_repeated_at_end as (
    select
        entry_id,
        entry_title,
        'title_repeated_at_end' as issue
    from {{ source('silver_tabletop', 'silver_entries') }}
    where entry_title is not null
      and length(entry_title) > 3
      and trim(content) like '%' || entry_title
      and trim(content) not like entry_title
),

chapter_refs_in_content as (
    -- Content references a ToC chapter by title inside parentheses
    select
        e.entry_id,
        e.entry_title,
        'chapter_ref_in_content' as issue
    from {{ source('silver_tabletop', 'silver_entries') }} e
    inner join {{ ref('silver_toc_sections') }} t
        on t.is_chapter = true
        and lower(e.content) like '%(' || lower(trim(split_part(t.title, ':', 1))) || ')%'
    where e.entry_title is not null
),

raw_html_in_content as (
    select
        entry_id,
        entry_title,
        'raw_html_in_content' as issue
    from {{ source('silver_tabletop', 'silver_entries') }}
    where content like '%<%>%'
)

select distinct * from title_repeated_at_end
union all
select distinct * from chapter_refs_in_content
union all
select distinct * from raw_html_in_content
