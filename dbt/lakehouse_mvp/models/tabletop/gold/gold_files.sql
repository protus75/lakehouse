-- Gold file metadata aggregated from chunks

select
    c.source_file,
    sf.total_pages,
    count(*) as total_chunks,
    count(distinct t.toc_id) as total_toc_entries,
    current_timestamp as built_at
from {{ ref('gold_chunks') }} c
join {{ ref('silver_files') }} sf on c.source_file = sf.source_file
join {{ ref('gold_toc') }} t on c.toc_id = t.toc_id
group by c.source_file, sf.total_pages
