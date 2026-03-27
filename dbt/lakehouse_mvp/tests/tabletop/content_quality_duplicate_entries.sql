-- Fail if multiple entries share the same (toc_title, entry_title) combination.
-- Duplicates indicate Marker page-boundary re-renders that orphan merge didn't catch,
-- or heading detection creating multiple entries from the same logical entry.
-- Excludes NULL entry_titles (chapter intros are expected to be untitled).
select
    toc_title,
    entry_title,
    count(*) as duplicate_count,
    string_agg(cast(entry_id as varchar), ', ' order by entry_id) as entry_ids
from {{ ref('silver_entries') }}
where entry_title is not null
group by toc_title, entry_title
having count(*) > 1
