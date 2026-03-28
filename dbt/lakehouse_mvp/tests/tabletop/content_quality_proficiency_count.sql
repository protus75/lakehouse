-- Fail if proficiency count in silver_entries doesn't match authority entries.
-- Every proficiency in the authority table should be found in the content.

with expected as (
    select count(*) as cnt from bronze_tabletop.authority_table_entries
    where entry_type = 'proficiency'
),
actual as (
    select count(distinct entry_title) as cnt from {{ ref('silver_entries') }} e
    inner join {{ ref('silver_toc_sections') }} t
        on e.toc_id = t.toc_id
    where lower(t.title) like '%proficiency desc%'
)

select
    e.cnt as expected_proficiencies,
    a.cnt as actual_proficiencies,
    e.cnt - a.cnt as missing
from expected e, actual a
where e.cnt != a.cnt
