-- Silver known entries: full index metadata from all appendixes
-- Cross-references Appendix 5 (school), 6 (sphere), 7 (class/level/page)

with raw_entries as (
    select
        source_file,
        entry_name,
        entry_class,
        entry_level,
        ref_page,
        source_section,
        school,
        sphere
    from {{ source('bronze_tabletop', 'known_entries_raw') }}
),

-- Merge school from Appendix 5 and sphere from Appendix 6 onto Appendix 7 entries
index_entries as (
    select
        r.source_file,
        r.entry_name,
        r.entry_class,
        r.entry_level,
        r.ref_page,
        coalesce(r.school, s5.school) as school,
        coalesce(r.sphere, s6.sphere) as sphere
    from raw_entries r
    left join raw_entries s5
        on r.source_file = s5.source_file
        and r.entry_name = s5.entry_name
        and s5.school is not null
    left join raw_entries s6
        on r.source_file = s6.source_file
        and r.entry_name = s6.entry_name
        and s6.sphere is not null
    where r.source_section like '%Spell Index%'
       or r.source_section like '%General Index%'
       or r.entry_class is not null
),

-- Deduplicate: keep the richest row per (name, class)
ranked as (
    select *,
        row_number() over (
            partition by source_file, entry_name, entry_class
            order by ref_page nulls last, school nulls last, sphere nulls last
        ) as rn
    from index_entries
)

select
    source_file,
    entry_name,
    entry_class,
    entry_level,
    ref_page,
    school,
    sphere
from ranked
where rn = 1
