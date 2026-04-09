"""Silver layer Python pipelines.

These are Dagster Python assets that read from bronze via get_reader() and
write to silver via write_iceberg() directly. They are NOT dbt models —
the dbt project handles SQL transforms only. Python transforms live here.

Why split: dbt's value is SQL templating, lineage docs, and the test
framework. Python models in dbt are pretending to be SQL models and gain
nothing from dbt's machinery. Moving them to plain Dagster assets removes
the dbt python-model adapter overhead, the temp parquet write, and the
publish-from-duckdb dance.
"""
