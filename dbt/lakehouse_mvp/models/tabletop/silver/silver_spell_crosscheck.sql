-- Silver spell cross-check: reconcile Appendix 1 (spell list), 5 (school), 6 (sphere), 7 (index)
-- Produces one authoritative row per spell with class, level, school, sphere, is_reversible
-- Uses index (Appendix 7) as primary, enriches with school/sphere/reversible from other appendixes

with index_spells as (
    -- Appendix 7: name, class, level, page (most authoritative for class + level)
    select
        source_file,
        entry_name,
        entry_class,
        entry_level,
        ref_page
    from {{ ref('silver_known_entries') }}
    where entry_class is not null
),

school_data as (
    -- Appendix 5: name → school
    select
        source_file,
        entry_name,
        school
    from {{ source('bronze_tabletop', 'known_entries_raw') }}
    where school is not null
),

sphere_data as (
    -- Appendix 6: name → sphere
    select
        source_file,
        entry_name,
        sphere
    from {{ source('bronze_tabletop', 'known_entries_raw') }}
    where sphere is not null
),

spell_list as (
    -- Appendix 1: name, class, level, is_reversible
    select
        source_file,
        entry_name,
        entry_class,
        entry_level,
        is_reversible
    from {{ source('bronze_tabletop', 'spell_list_entries') }}
),

-- Join everything onto the index (ground truth for name + class + level)
combined as (
    select
        i.source_file,
        i.entry_name,
        i.entry_class,
        i.entry_level,
        i.ref_page,
        s5.school,
        s6.sphere,
        sl.is_reversible,
        -- Cross-check flags
        case when sl.entry_name is not null then true else false end as in_spell_list,
        case when s5.entry_name is not null then true else false end as in_school_index,
        case when s6.entry_name is not null then true else false end as in_sphere_index,
        case when sl.entry_level is not null and sl.entry_level != i.entry_level
            then true else false end as level_mismatch
    from index_spells i
    left join school_data s5
        on i.source_file = s5.source_file and i.entry_name = s5.entry_name
    left join sphere_data s6
        on i.source_file = s6.source_file and i.entry_name = s6.entry_name
    left join spell_list sl
        on i.source_file = sl.source_file and i.entry_name = sl.entry_name
        and i.entry_class = sl.entry_class
)

select * from combined
