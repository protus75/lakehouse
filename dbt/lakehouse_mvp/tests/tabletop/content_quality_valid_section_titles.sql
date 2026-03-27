-- Fail if any entry has a section_title not found in ToC sections and not used
-- as a sub-section by multiple entries (which would indicate a legitimate heading).
-- Catches Marker rendering random content as H1/H2 that leaked through as sections.
with legitimate_sub_sections as (
    -- A section_title used by 2+ entries in the same chapter is a real sub-section
    select distinct section_title
    from {{ ref('silver_entries') }}
    where section_title is not null
    group by toc_title, section_title
    having count(*) >= 2
)

select
    e.entry_id,
    e.toc_title,
    e.section_title,
    e.entry_title
from {{ ref('silver_entries') }} e
where e.section_title is not null
  and not exists (
      select 1 from {{ ref('silver_toc_sections') }} t
      where lower(e.section_title) = lower(t.title)
         or lower(e.section_title) = lower(trim(split_part(t.title, ':', 2)))
  )
  and e.section_title not in (select section_title from legitimate_sub_sections)
