-- Fail if entries have section_title that doesn't match their toc_title chapter
-- e.g. Chapter 11 entries should not have section_title "Chapter 10"
select
    entry_id,
    toc_title,
    section_title,
    entry_title
from {{ ref('silver_entries') }}
where section_title like 'Chapter %'
  and toc_title like 'Chapter %'
  and section_title != split_part(toc_title, ':', 1)
  and split_part(toc_title, ':', 1) not like '%' || section_title || '%'
