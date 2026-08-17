-- Runs automatically on first container startup (mounted into
-- /docker-entrypoint-initdb.d/). POSTGRES_DB env var already creates the
-- main warehouse DB (ecommerce_dw); this creates Airflow's separate
-- metadata DB so the two never mix tables.

CREATE DATABASE airflow_meta;
