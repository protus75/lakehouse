-- Fail if an entry's toc_title doesn't match the title of its toc_id section.
-- Catches entries where toc_id and toc_title are out of sync.
select
    e.entry_id,
    e.toc_id,
    e.toc_title as entry_toc_title,
    t.title as actual_toc_title,
    e.entry_title
from {{ source('silver_tabletop', 'silver_entries') }} e
join {{ ref('silver_toc_sections') }} t
    on e.toc_id = t.toc_id
where e.toc_title != t.title
