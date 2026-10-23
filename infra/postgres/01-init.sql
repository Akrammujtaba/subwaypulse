-- One Postgres for three jobs: the Iceberg catalog, reference data, and Airflow.
CREATE DATABASE iceberg;
CREATE DATABASE airflow;
\connect subway
CREATE SCHEMA IF NOT EXISTS ref;
