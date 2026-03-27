-- Fail if any ToC table entry has no matching parsed table in tables_raw.
-- Every table listed in the ToC should be extracted.
select
    t.title,
    t.page_start
from {{ ref('silver_toc_sections') }} t
where t.is_table = true
  and t.is_excluded = false
  and regexp_extract(t.title, 'Table\s+(\d+)', 1) != ''
  and not exists (
      select 1 from {{ source('bronze_tabletop', 'tables_raw') }} r
      where r.table_number = cast(
          regexp_extract(t.title, 'Table\s+(\d+)', 1) as integer
      )
  )
