{% macro create_bronze_views() %}
    {# Create DuckDB views over Iceberg tables so dbt models can read them.
       Also creates views for Dagster-owned silver tables (silver_entries)
       that dbt models reference but don't materialize. #}

    {% set bronze_tables = [
        'files', 'marker_extractions', 'page_texts', 'toc_raw',
        'known_entries_raw', 'spell_list_entries', 'watermarks',
        'tables_raw', 'authority_table_entries',
        'pipeline_runs', 'catalog'
    ] %}

    {# Dagster-owned silver tables that exist in iceberg but not in dbt.
       dbt models like silver_files read from these via raw SQL. #}
    {% set silver_iceberg_tables = [
        'silver_entries'
    ] %}

    {% set warehouse = var('warehouse_path') %}

    CREATE SCHEMA IF NOT EXISTS bronze_tabletop;

    {% for table in bronze_tables %}
        DROP VIEW IF EXISTS bronze_tabletop.{{ table }};
        DROP TABLE IF EXISTS bronze_tabletop.{{ table }};
        CREATE VIEW bronze_tabletop.{{ table }} AS
        SELECT * FROM iceberg_scan('{{ warehouse }}/bronze_tabletop/{{ table }}');
    {% endfor %}

    CREATE SCHEMA IF NOT EXISTS silver_tabletop;

    {% for table in silver_iceberg_tables %}
        DROP VIEW IF EXISTS silver_tabletop.{{ table }};
        DROP TABLE IF EXISTS silver_tabletop.{{ table }};
        CREATE VIEW silver_tabletop.{{ table }} AS
        SELECT * FROM iceberg_scan('{{ warehouse }}/silver_tabletop/{{ table }}');
    {% endfor %}
{% endmacro %}
