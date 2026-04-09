-- Fail if multiple entries share the same (toc_id, entry_title) combination.
-- Duplicates indicate page-boundary re-renders or heading detection
-- creating multiple entries from the same logical entry.
-- Excludes NULL entry_titles (chapter intros are expected to be untitled).
select
    toc_id,
    toc_title,
    entry_title,
    count(*) as duplicate_count,
    string_agg(cast(entry_id as varchar), ', ' order by entry_id) as entry_ids
from {{ source('silver_tabletop', 'silver_entries') }}
where entry_title is not null
group by toc_id, toc_title, entry_title
having count(*) > 1
