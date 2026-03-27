-- Fail if any non-excluded chapter-level ToC entry has zero matching silver_entries.
-- Entries are assigned to chapters via toc_title, so only chapters are checked here.
-- Sub-section coverage is validated separately via entry_title matching.
with chapter_counts as (
    select
        t.toc_id,
        t.title,
        count(e.entry_id) as entry_count
    from {{ ref('silver_toc_sections') }} t
    left join {{ ref('silver_entries') }} e
        on e.toc_title = t.title
    where t.is_excluded = false
      and t.is_chapter = true
      and t.is_table = false
    group by t.toc_id, t.title
),

-- Sub-sections: check if the title appears as an entry_title or section_title
sub_section_missing as (
    select
        t.toc_id,
        t.title,
        0 as entry_count
    from {{ ref('silver_toc_sections') }} t
    where t.is_excluded = false
      and t.is_chapter = false
      and t.is_table = false
      and not exists (
          select 1 from {{ ref('silver_entries') }} e
          where lower(e.entry_title) = lower(t.title)
             or lower(e.section_title) = lower(t.title)
      )
)

select * from chapter_counts where entry_count = 0
union all
select * from sub_section_missing
