-- Gold: clean entry descriptions from silver, ready for AI enrichment.
-- The enrichment script appends rows with description_type = 'summary'.

SELECT
    entry_id,
    source_file,
    description_type,
    content
FROM {{ ref('silver_entry_descriptions') }}
