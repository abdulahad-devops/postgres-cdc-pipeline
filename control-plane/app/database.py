import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

CONTROL_DSN = os.getenv(
    "CONTROL_DATABASE_URL",
    "postgresql://cdc_control:cdc_control@control-postgres:5432/cdc_control",
)

DDL = """
CREATE TABLE IF NOT EXISTS tenants (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email text NOT NULL UNIQUE,
    password_salt bytea NOT NULL,
    password_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_connections (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    host text NOT NULL,
    port integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
    database_name text NOT NULL,
    database_user text NOT NULL,
    sslmode text NOT NULL CHECK (sslmode IN ('require', 'verify-ca', 'verify-full')),
    secret_ref text NOT NULL,
    connector_name text NOT NULL UNIQUE,
    topic_prefix text NOT NULL UNIQUE,
    publication_name text NOT NULL UNIQUE,
    slot_name text NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'draft',
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS selected_tables (
    connection_id uuid NOT NULL REFERENCES source_connections(id) ON DELETE CASCADE,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    primary_key_columns text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (connection_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS replica_records (
    connection_id uuid NOT NULL REFERENCES source_connections(id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    key_hash text NOT NULL,
    record_key jsonb NOT NULL,
    record_data jsonb NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false,
    source_lsn text,
    last_event_id text NOT NULL,
    replicated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    PRIMARY KEY (connection_id, schema_name, table_name, key_hash)
);

CREATE TABLE IF NOT EXISTS delete_audit (
    id bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    connection_id uuid NOT NULL REFERENCES source_connections(id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    record_key jsonb NOT NULL,
    record_data jsonb NOT NULL,
    source_lsn text,
    protected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS replica_records_tenant_table_idx
ON replica_records (tenant_id, connection_id, schema_name, table_name, is_deleted);

CREATE INDEX IF NOT EXISTS delete_audit_tenant_idx
ON delete_audit (tenant_id, protected_at DESC);
"""


@contextmanager
def db():
    with psycopg.connect(CONTROL_DSN, row_factory=dict_row) as connection:
        yield connection


def migrate() -> None:
    with db() as connection:
        connection.execute(DDL)
        connection.commit()
