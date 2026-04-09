-- silver_entries.entry_id must be unique. Replaces the unique test from
-- the silver_entries dbt model that was removed when silver_entries became
-- a Dagster Python asset.
select entry_id, count(*) as n
from {{ source('silver_tabletop', 'silver_entries') }}
group by entry_id
having count(*) > 1
