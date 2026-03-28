-- Spells with no school parsed from content header.
-- Every spell should have at least one school in parentheses on the first line.

select
    m.entry_id,
    e.entry_title,
    m.school
from {{ ref('silver_spell_meta') }} m
join {{ ref('silver_entries') }} e on e.entry_id = m.entry_id
where m.school = '' or m.school is null
