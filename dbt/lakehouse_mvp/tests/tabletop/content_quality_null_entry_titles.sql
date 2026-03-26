-- Fail if more than 5% of chunks have NULL entry_title (orphaned content)
-- Some NULL titles are expected for chapter intro text, but >5% indicates
-- heading detection problems
with stats as (
    select
        count(*) as total_chunks,
        sum(case when entry_title is null then 1 else 0 end) as null_titles
    from {{ ref('gold_chunks') }}
)
select *
from stats
where null_titles > total_chunks * 0.05
