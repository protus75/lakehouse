-- Fail if any entry_title matches a chapter-level ToC title, EXCLUDING
-- chapter intro entries (where entry_title = toc_title, which is expected
-- for the chapter's own introductory content).
select
    e.entry_id,
    e.toc_title,
    e.entry_title,
    e.char_count
from {{ source('silver_tabletop', 'silver_entries') }} e
inner join {{ ref('silver_toc_sections') }} t
    on t.is_chapter = true
    and lower(e.entry_title) = lower(t.title)
where e.entry_title is not null
  -- Exclude chapter intros: entry whose toc_id IS that chapter
  and e.toc_id != t.toc_id
