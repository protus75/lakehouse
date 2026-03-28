-- Fail if any non-excluded, non-table ToC section has zero rows in gold_entries.
-- Every section should have at least one entry (even if just a heading).

select
    t.toc_id,
    t.title,
    t.sort_order
from {{ ref('gold_toc') }} t
left join {{ ref('gold_entries') }} e
    on e.toc_id = t.toc_id
where t.is_excluded = false
  and t.is_table = false
  and e.toc_id is null
