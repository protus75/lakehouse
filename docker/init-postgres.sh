#!/bin/bash
set -e

# Create additional databases beyond the default ($POSTGRES_DB = iceberg).
# Dagster needs its own database on the same PostgreSQL instance.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE dagster;
EOSQL
