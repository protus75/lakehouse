{% macro create_bronze_views() %}
    {# Create DuckDB views over Iceberg tables.
       Drops old tables/views with the same name first. #}

    {% set bronze_tables = [
        'files', 'marker_extractions', 'page_texts', 'toc_raw',
        'known_entries_raw', 'spell_list_entries', 'watermarks',
        'tables_raw', 'authority_table_entries',
        'pipeline_runs', 'catalog'
    ] %}

    {% set warehouse = var('warehouse_path') %}

    CREATE SCHEMA IF NOT EXISTS bronze_tabletop;

    {% for table in bronze_tables %}
        DROP VIEW IF EXISTS bronze_tabletop.{{ table }};
        DROP TABLE IF EXISTS bronze_tabletop.{{ table }};
        CREATE VIEW bronze_tabletop.{{ table }} AS
        SELECT * FROM iceberg_scan('{{ warehouse }}/bronze_tabletop/{{ table }}');
    {% endfor %}
{% endmacro %}
