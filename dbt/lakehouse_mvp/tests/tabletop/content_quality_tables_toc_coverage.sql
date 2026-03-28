-- Fail if any ToC table entry has no matching parsed table in tables_raw.
-- Every is_table entry in the validated ToC should be extracted.
-- Matches on toc_title (full ToC title) — works for both numbered and unlabeled tables.
select
    t.title,
    t.page_start
from {{ ref('silver_toc_sections') }} t
where t.is_table = true
  and t.is_excluded = false
  and not exists (
      select 1 from {{ source('bronze_tabletop', 'tables_raw') }} r
      where r.toc_title = t.title
  )
