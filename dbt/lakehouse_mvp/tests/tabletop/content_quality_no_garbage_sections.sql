-- Fail if any silver entries have section_title that doesn't match any ToC section
-- Validates structurally: section_title must appear as a word sequence in some
-- ToC title, or match a known sub-section pattern (e.g. "First-Level Spells")
select
    e.entry_id,
    e.toc_title,
    e.section_title,
    e.entry_title
from {{ ref('silver_entries') }} e
left join {{ ref('silver_toc_sections') }} t
    on (
        lower(e.section_title) = lower(t.title)
        or lower(t.title) like '%' || lower(e.section_title) || '%'
        or lower(e.section_title) like '%' || lower(split_part(t.title, ':', 2)) || '%'
        or e.section_title similar to '(First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth)-Level Spells'
    )
where e.section_title is not null
  and t.toc_id is null
