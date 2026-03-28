-- Gold entries: full book content in ToC reading order for browser display.
-- ToC drives the order. Entries are attached to their ToC section via toc_id.
-- Sections with no entries still appear (for headings/anchors).

select
    e.entry_id,
    t.source_file,
    t.toc_id,
    t.title as toc_title,
    e.section_title,
    e.entry_title,
    e.content,
    e.char_count,
    e.spell_class,
    e.spell_level,
    t.sort_order,
    t.depth,
    t.is_chapter,
    t.is_table,
    t.is_excluded
from {{ ref('silver_toc_sections') }} t
left join {{ ref('silver_entries') }} e
    on e.source_file = t.source_file
    and e.toc_id = t.toc_id
where t.is_excluded = false
order by t.source_file, t.sort_order, e.entry_id
