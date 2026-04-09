-- silver_entries required fields must be present. Replaces the not_null
-- column tests from the dbt model that was removed when silver_entries
-- became a Dagster Python asset.
select *
from {{ source('silver_tabletop', 'silver_entries') }}
where entry_id is null
   or source_file is null
   or toc_title is null
   or section_title is null
   or content is null
