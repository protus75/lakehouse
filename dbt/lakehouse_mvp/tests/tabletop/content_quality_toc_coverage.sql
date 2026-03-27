{{ config(severity='warn') }}
-- Fail if any non-excluded ToC entry has zero matching content in gold_chunks.
-- Every ToC section should have at least one chunk with a matching entry_title,
-- or be a parent chapter with child content.

with toc_entries as (
    select
        t.toc_id,
        t.title,
        t.is_chapter,
        t.is_table,
        t.parent_title
    from {{ ref('gold_toc') }} t
    where t.is_excluded = false
),

-- Count chunks that match each ToC entry by title
direct_matches as (
    select
        t.toc_id,
        t.title,
        t.is_chapter,
        count(c.chunk_id) as chunk_count
    from toc_entries t
    left join {{ ref('gold_chunks') }} c
        on c.entry_title = t.title
    group by t.toc_id, t.title, t.is_chapter
),

-- Chapters also get credit for having any chunks via toc_id
chapter_chunks as (
    select
        t.toc_id,
        count(c.chunk_id) as chapter_chunk_count
    from toc_entries t
    left join {{ ref('gold_chunks') }} c
        on c.toc_id = t.toc_id
    where t.is_chapter = true
    group by t.toc_id
),

-- Find ToC entries with no content at all
missing as (
    select
        d.toc_id,
        d.title,
        d.is_chapter,
        d.chunk_count,
        coalesce(cc.chapter_chunk_count, 0) as chapter_chunk_count
    from direct_matches d
    left join chapter_chunks cc on d.toc_id = cc.toc_id
    where d.chunk_count = 0
      and coalesce(cc.chapter_chunk_count, 0) = 0
)

select * from missing
