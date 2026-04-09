-- silver_entries.toc_id must reference a row in silver_toc_sections.
-- Replaces the relationships test from the dbt model that was removed
-- when silver_entries became a Dagster Python asset.
select e.entry_id, e.toc_id
from {{ source('silver_tabletop', 'silver_entries') }} e
left join {{ ref('silver_toc_sections') }} t
  on e.toc_id = t.toc_id
where t.toc_id is null
