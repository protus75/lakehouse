-- Silver known entries with validation flag

with raw_entries as (
    select
        source_file,
        entry_name
    from {{ source('bronze_tabletop', 'known_entries_raw') }}
),

-- Check which known entries actually appear as entry_titles in silver
validated as (
    select distinct
        source_file,
        lower(entry_title) as entry_name_lower
    from {{ ref('silver_entries') }}
    where entry_title is not null
)

select
    r.source_file,
    r.entry_name,
    case when v.entry_name_lower is not null then true else false end as is_validated
from raw_entries r
left join validated v
    on r.source_file = v.source_file
    and r.entry_name = v.entry_name_lower
