import re
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .secrets_store import get_connection_secret

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def validate_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe PostgreSQL identifier: {value!r}")
    return value


def _kwargs(connection: dict[str, Any]) -> dict[str, Any]:
    secret = get_connection_secret(connection["secret_ref"])
    return {
        "host": connection["host"],
        "port": connection["port"],
        "dbname": connection["database_name"],
        "user": connection["database_user"],
        "password": secret["password"],
        "sslmode": connection["sslmode"],
        "connect_timeout": 8,
    }


@contextmanager
def source_db(connection: dict[str, Any]):
    with psycopg.connect(**_kwargs(connection), row_factory=dict_row) as client:
        yield client


def test_connection(connection: dict[str, Any], password: str) -> dict[str, str]:
    kwargs = {
        "host": connection["host"],
        "port": connection["port"],
        "dbname": connection["database_name"],
        "user": connection["database_user"],
        "password": password,
        "sslmode": connection["sslmode"],
        "connect_timeout": 8,
    }
    with psycopg.connect(**kwargs, row_factory=dict_row) as client:
        row = client.execute(
            "SELECT current_database() AS database, current_user AS username, version() AS version"
        ).fetchone()
        return row


def discover_tables(connection: dict[str, Any]) -> list[dict[str, Any]]:
    query = """
    SELECT
      ns.nspname AS schema_name,
      cls.relname AS table_name,
      COALESCE((
        SELECT json_agg(att.attname ORDER BY keys.ordinality)
        FROM pg_index idx
        CROSS JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
        JOIN pg_attribute att
          ON att.attrelid = idx.indrelid AND att.attnum = keys.attnum
        WHERE idx.indrelid = cls.oid AND idx.indisprimary
      ), '[]'::json) AS primary_key_columns
    FROM pg_class cls
    JOIN pg_namespace ns ON ns.oid = cls.relnamespace
    WHERE cls.relkind IN ('r', 'p')
      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
      AND ns.nspname NOT LIKE 'pg_toast%'
    ORDER BY ns.nspname, cls.relname
    """
    with source_db(connection) as client:
        return list(client.execute(query).fetchall())


def table_columns(client, schema_name: str, table_name: str) -> list[dict[str, Any]]:
    return list(
        client.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default, is_identity, is_generated
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        ).fetchall()
    )


def read_source_rows(connection: dict[str, Any], schema_name: str, table_name: str, limit: int) -> dict[str, Any]:
    validate_identifier(schema_name)
    validate_identifier(table_name)
    with source_db(connection) as client:
        columns = table_columns(client, schema_name, table_name)
        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            sql.Identifier(schema_name), sql.Identifier(table_name)
        )
        rows = list(client.execute(query, (limit,)).fetchall())
        return {"columns": columns, "rows": rows}


def insert_source_row(connection: dict[str, Any], schema_name: str, table_name: str, values: dict[str, Any]):
    if not values:
        raise ValueError("At least one column is required")
    validate_identifier(schema_name)
    validate_identifier(table_name)
    columns = [validate_identifier(name) for name in values]
    query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING *").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    with source_db(connection) as client:
        row = client.execute(query, [values[name] for name in columns]).fetchone()
        client.commit()
        return row


def _where_from_key(primary_key: dict[str, Any], required_columns: list[str]):
    if set(primary_key) != set(required_columns):
        raise ValueError(f"Primary key must contain exactly: {', '.join(required_columns)}")
    for name in primary_key:
        validate_identifier(name)
    predicate = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder()) for name in required_columns
    )
    params = [primary_key[name] for name in required_columns]
    return predicate, params


def update_source_row(
    connection: dict[str, Any],
    schema_name: str,
    table_name: str,
    values: dict[str, Any],
    primary_key: dict[str, Any],
    required_pk: list[str],
):
    if not values:
        raise ValueError("At least one changed column is required")
    columns = [validate_identifier(name) for name in values]
    predicate, key_params = _where_from_key(primary_key, required_pk)
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder()) for name in columns
    )
    query = sql.SQL("UPDATE {}.{} SET {} WHERE {} RETURNING *").format(
        sql.Identifier(schema_name), sql.Identifier(table_name), assignments, predicate
    )
    with source_db(connection) as client:
        row = client.execute(query, [values[name] for name in columns] + key_params).fetchone()
        if row is None:
            raise LookupError("Row not found")
        client.commit()
        return row


def delete_source_row(
    connection: dict[str, Any],
    schema_name: str,
    table_name: str,
    primary_key: dict[str, Any],
    required_pk: list[str],
):
    predicate, params = _where_from_key(primary_key, required_pk)
    query = sql.SQL("DELETE FROM {}.{} WHERE {} RETURNING *").format(
        sql.Identifier(schema_name), sql.Identifier(table_name), predicate
    )
    with source_db(connection) as client:
        row = client.execute(query, params).fetchone()
        if row is None:
            raise LookupError("Row not found")
        client.commit()
        return row
