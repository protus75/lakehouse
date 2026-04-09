-- Fail if any entry has a toc_id that doesn't exist in silver_toc_sections.
select
    e.entry_id,
    e.toc_id,
    e.toc_title,
    e.entry_title
from {{ source('silver_tabletop', 'silver_entries') }} e
left join {{ ref('silver_toc_sections') }} t
    on e.toc_id = t.toc_id
where t.toc_id is null
