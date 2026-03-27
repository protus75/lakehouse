-- Fail if any non-excluded chapter has zero silver_entries.
-- Only checks chapter-level coverage. Sub-sections are paragraph-level divisions
-- that Marker renders as flowing text within parent entries — they are not
-- independently extractable as separate entries.
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
having count(e.entry_id) = 0
