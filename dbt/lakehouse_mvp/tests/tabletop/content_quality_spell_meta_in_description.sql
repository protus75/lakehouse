-- Spell descriptions that still contain metadata field patterns.
-- If "Range:", "Duration:", etc. appear in the description, the header
-- stripping may have failed (blank line in wrong place, or metadata
-- continued past expected boundary).

select
    d.entry_id,
    e.entry_title,
    d.content
from {{ ref('silver_entry_descriptions') }} d
join {{ ref('silver_entries') }} e on e.entry_id = d.entry_id
where e.spell_level is not null
  and d.description_type = 'original'
  and (
    d.content like '%Range:%'
    or d.content like '%Duration:%'
    or d.content like '%Casting Time:%'
    or d.content like '%Area of Effect:%'
    or d.content like '%Saving Throw:%'
    or d.content like '%Components:%'
    or d.content like '%Sphere:%'
  )
