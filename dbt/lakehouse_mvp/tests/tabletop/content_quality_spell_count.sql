-- Fail if spell count in silver_entries doesn't match spell_list_entries.
-- Every spell in the index should be found in the content.

with expected as (
    select count(*) as cnt from bronze_tabletop.spell_list_entries
),
actual as (
    select count(*) as cnt from {{ ref('silver_entries') }}
    where spell_class is not null
)

select
    e.cnt as expected_spells,
    a.cnt as actual_spells,
    e.cnt - a.cnt as missing
from expected e, actual a
where e.cnt != a.cnt
