-- Spells missing required metadata fields.
-- Every spell should have range, duration, casting_time, and area_of_effect.
-- Saving_throw and components are optional (some spells omit them).

select
    m.entry_id,
    e.entry_title,
    m.range,
    m.duration,
    m.casting_time,
    m.area_of_effect
from {{ ref('silver_spell_meta') }} m
join {{ source('silver_tabletop', 'silver_entries') }} e on e.entry_id = m.entry_id
where m.range = ''
   or m.duration = ''
   or m.casting_time = ''
   or m.area_of_effect = ''
