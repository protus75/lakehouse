-- Fail if any silver entries have section_title that doesn't match any ToC section.
-- Uses same logic as valid_section_titles but with broader matching for backwards compat.
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
        or lower(e.section_title) like '%' || lower(trim(split_part(t.title, ':', 2))) || '%'
    )
where e.section_title is not null
  and t.toc_id is null
