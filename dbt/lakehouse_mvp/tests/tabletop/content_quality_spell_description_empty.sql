-- Spell entries where stripping the metadata header left no description.
-- Every spell should have prose content after the metadata block.

select
    d.entry_id,
    e.entry_title,
    length(d.content) as desc_length
from {{ ref('silver_entry_descriptions') }} d
join {{ ref('silver_entries') }} e on e.entry_id = d.entry_id
where e.spell_level is not null
  and d.description_type = 'original'
  and (d.content is null or length(d.content) < 20)
