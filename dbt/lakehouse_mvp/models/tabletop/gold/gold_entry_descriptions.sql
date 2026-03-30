-- Gold: clean entry descriptions from silver.
-- AI summaries are in a separate table (gold_ai_summaries) owned by enrichment.

SELECT
    d.entry_id,
    d.source_file,
    i.entry_type,
    d.content
FROM {{ ref('silver_entry_descriptions') }} d
JOIN {{ ref('gold_entry_index') }} i ON d.entry_id = i.entry_id
