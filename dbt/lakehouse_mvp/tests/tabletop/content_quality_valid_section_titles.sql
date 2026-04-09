-- Fail if any entry has a section_title that doesn't match its own entry_title
-- AND doesn't match any ToC section title. This catches garbage section_titles
-- from OCR artifacts leaking through as headings.
-- Entries where section_title = entry_title are normal (section_title echoes the entry name).
select
    e.entry_id,
    e.toc_title,
    e.section_title,
    e.entry_title
from {{ source('silver_tabletop', 'silver_entries') }} e
where e.section_title is not null
  and e.section_title != coalesce(e.entry_title, '')
  and not exists (
      select 1 from {{ ref('silver_toc_sections') }} t
      where lower(e.section_title) = lower(t.title)
         or lower(e.section_title) = lower(trim(split_part(t.title, ': ', 2)))
  )
