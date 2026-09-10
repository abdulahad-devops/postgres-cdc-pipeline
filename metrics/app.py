import json
import os
import time
import urllib.request

import psycopg
from prometheus_client import Gauge, start_http_server


SOURCE_DSN = os.getenv(
    "SOURCE_DSN",
    "postgresql://postgres:postgres@source-postgres:5432/source_db",
)
REPLICA_DSN = os.getenv(
    "REPLICA_DSN",
    "postgresql://postgres:postgres@replica-postgres:5432/replica_db",
)
DEBEZIUM_STATUS_URL = os.getenv(
    "DEBEZIUM_STATUS_URL",
    "http://debezium:8083/connectors/orders-source-connector/status",
)
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))


source_database_up = Gauge(
    "cdc_source_database_up",
    "Whether the source PostgreSQL database can be queried",
)
replica_database_up = Gauge(
    "cdc_replica_database_up",
    "Whether the replica PostgreSQL database can be queried",
)
debezium_connector_up = Gauge(
    "cdc_debezium_connector_up",
    "Whether the Debezium connector state is RUNNING",
)
debezium_task_up = Gauge(
    "cdc_debezium_task_up",
    "Whether all Debezium connector tasks are RUNNING",
)
replication_slot_present = Gauge(
    "cdc_replication_slot_present",
    "Whether the expected PostgreSQL replication slot exists",
)
replication_slot_lag_bytes = Gauge(
    "cdc_replication_slot_lag_bytes",
    "Bytes between the current WAL position and the connector confirmed flush LSN",
)
source_row_count = Gauge(
    "cdc_source_orders_rows",
    "Number of rows in the source orders table",
)
replica_row_count = Gauge(
    "cdc_replica_orders_rows",
    "Number of rows in the replica orders table",
)
row_count_difference = Gauge(
    "cdc_orders_row_count_difference",
    "Absolute difference between source and replica orders row counts",
)
end_to_end_latency_seconds = Gauge(
    "cdc_end_to_end_latency_seconds",
    "Latency of the latest replica write compared with its source updated_at timestamp",
)
last_replica_write_timestamp_seconds = Gauge(
    "cdc_last_replica_write_timestamp_seconds",
    "Unix timestamp of the latest write applied to the replica orders table",
)


def collect_source_metrics():
    with psycopg.connect(SOURCE_DSN, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM public.orders")
            rows = float(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT
                  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::double precision
                FROM pg_replication_slots
                WHERE slot_name = 'orders_cdc_slot'
                """
            )
            slot_row = cursor.fetchone()

    source_database_up.set(1)
    source_row_count.set(rows)

    if slot_row is None:
        replication_slot_present.set(0)
        replication_slot_lag_bytes.set(0)
    else:
        replication_slot_present.set(1)
        replication_slot_lag_bytes.set(float(slot_row[0] or 0))

    return rows


def collect_replica_metrics():
    with psycopg.connect(REPLICA_DSN, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM public.orders")
            rows = float(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT
                  GREATEST(
                    EXTRACT(EPOCH FROM (replicated_at - updated_at)),
                    0
                  )::double precision,
                  EXTRACT(EPOCH FROM replicated_at)::double precision
                FROM public.orders
                ORDER BY replicated_at DESC
                LIMIT 1
                """
            )
            latest_row = cursor.fetchone()

    replica_database_up.set(1)
    replica_row_count.set(rows)

    if latest_row is None:
        end_to_end_latency_seconds.set(0)
        last_replica_write_timestamp_seconds.set(0)
    else:
        end_to_end_latency_seconds.set(float(latest_row[0] or 0))
        last_replica_write_timestamp_seconds.set(float(latest_row[1] or 0))

    return rows


def collect_debezium_metrics():
    request = urllib.request.Request(DEBEZIUM_STATUS_URL)
    with urllib.request.urlopen(request, timeout=3) as response:
        status = json.load(response)

    connector_running = status.get("connector", {}).get("state") == "RUNNING"
    tasks = status.get("tasks", [])
    tasks_running = bool(tasks) and all(task.get("state") == "RUNNING" for task in tasks)

    debezium_connector_up.set(1 if connector_running else 0)
    debezium_task_up.set(1 if tasks_running else 0)


def collect_once():
    source_rows = None
    replica_rows = None

    try:
        source_rows = collect_source_metrics()
    except Exception as error:
        source_database_up.set(0)
        replication_slot_present.set(0)
        print(f"source metrics error: {error}", flush=True)

    try:
        replica_rows = collect_replica_metrics()
    except Exception as error:
        replica_database_up.set(0)
        print(f"replica metrics error: {error}", flush=True)

    if source_rows is not None and replica_rows is not None:
        row_count_difference.set(abs(source_rows - replica_rows))

    try:
        collect_debezium_metrics()
    except Exception as error:
        debezium_connector_up.set(0)
        debezium_task_up.set(0)
        print(f"Debezium metrics error: {error}", flush=True)


if __name__ == "__main__":
    start_http_server(8000)
    print("CDC metrics exporter listening on port 8000", flush=True)

    while True:
        collect_once()
        time.sleep(POLL_INTERVAL_SECONDS)
