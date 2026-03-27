-- Fail if any entry_title matches a chapter-level ToC title or its descriptive part.
-- These are cross-references in content that heading detection treated as entries.
select
    e.entry_id,
    e.toc_title,
    e.entry_title,
    e.char_count
from {{ ref('silver_entries') }} e
inner join {{ ref('silver_toc_sections') }} t
    on t.is_chapter = true
    and (
        lower(e.entry_title) = lower(t.title)
        or lower(e.entry_title) = lower(trim(split_part(t.title, ': ', 2)))
    )
where e.entry_title is not null
