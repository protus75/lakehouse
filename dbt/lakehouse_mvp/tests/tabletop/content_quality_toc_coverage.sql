-- Fail if any non-excluded, non-table ToC section has zero entries in gold_entries,
-- excluding parent sections whose children have entries.

with gaps as (
    select
        g.toc_id,
        g.toc_title,
        g.is_chapter
    from {{ ref('gold_entries') }} g
    where g.entry_id is null
      and g.is_table = false
),

-- Parent sections with child entries (e.g. "Wizard Spells" has level sub-sections)
parents_with_children as (
    select distinct t.parent_title
    from {{ ref('silver_toc_sections') }} t
    join {{ ref('silver_entries') }} e on e.toc_id = t.toc_id
    where t.parent_title is not null
)

select g.*
from gaps g
where g.toc_title not in (select parent_title from parents_with_children)
