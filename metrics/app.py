import json
import os
import time
import urllib.request
from pathlib import Path

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
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "/backups"))
NAN = float("nan")


source_database_up = Gauge(
    "cdc_source_database_up",
    "Whether the source PostgreSQL database can be queried",
)
replica_database_up = Gauge(
    "cdc_replica_database_up",
    "Whether the replica PostgreSQL database can be queried",
)
source_orders_table_present = Gauge(
    "cdc_source_orders_table_present",
    "Whether public.orders exists in the source database",
)
replica_orders_table_present = Gauge(
    "cdc_replica_orders_table_present",
    "Whether public.orders exists in the replica database",
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
    "Absolute raw row-count difference; protected deletes can make this non-zero",
)
protected_replica_rows = Gauge(
    "cdc_protected_replica_rows",
    "Replica order IDs that no longer exist in the source",
)
active_rows_missing = Gauge(
    "cdc_active_source_rows_missing_from_replica",
    "Source order IDs that are missing from the replica",
)
active_rows_mismatched = Gauge(
    "cdc_active_source_rows_mismatched_in_replica",
    "Source orders whose business values do not match the replica",
)
protected_delete_events = Gauge(
    "cdc_protected_delete_events_total",
    "Durable number of source delete events recorded without deleting replica rows",
)
last_protected_delete_timestamp_seconds = Gauge(
    "cdc_last_protected_delete_timestamp_seconds",
    "Unix timestamp when the latest protected delete was recorded",
)
end_to_end_latency_seconds = Gauge(
    "cdc_end_to_end_latency_seconds",
    "Latency of the latest replica write compared with its source updated_at timestamp",
)
last_replica_write_timestamp_seconds = Gauge(
    "cdc_last_replica_write_timestamp_seconds",
    "Unix timestamp of the latest write applied to the replica orders table",
)
backup_present = Gauge(
    "cdc_backup_present",
    "Whether at least one completed replica backup file exists",
)
backup_files_total = Gauge(
    "cdc_backup_files_total",
    "Number of completed replica backup files retained",
)
last_backup_timestamp_seconds = Gauge(
    "cdc_last_backup_timestamp_seconds",
    "Filesystem modification time of the latest completed replica backup",
)
last_backup_age_seconds = Gauge(
    "cdc_last_backup_age_seconds",
    "Age in seconds of the latest completed replica backup",
)


def collect_source_metrics():
    records = None

    with psycopg.connect(SOURCE_DSN, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.orders') IS NOT NULL")
            table_exists = bool(cursor.fetchone()[0])
            source_database_up.set(1)
            source_orders_table_present.set(1 if table_exists else 0)

            if table_exists:
                cursor.execute(
                    """
                    SELECT id, customer_name, amount, status, updated_at
                    FROM public.orders
                    """
                )
                records = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}
                source_row_count.set(len(records))
            else:
                source_row_count.set(NAN)

            cursor.execute(
                """
                SELECT
                  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::double precision
                FROM pg_replication_slots
                WHERE slot_name = 'orders_cdc_slot'
                """
            )
            slot_row = cursor.fetchone()

    if slot_row is None:
        replication_slot_present.set(0)
        replication_slot_lag_bytes.set(0)
    else:
        replication_slot_present.set(1)
        replication_slot_lag_bytes.set(float(slot_row[0] or 0))

    return records


def collect_replica_metrics():
    records = None

    with psycopg.connect(REPLICA_DSN, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.orders') IS NOT NULL")
            table_exists = bool(cursor.fetchone()[0])
            replica_database_up.set(1)
            replica_orders_table_present.set(1 if table_exists else 0)

            if table_exists:
                cursor.execute(
                    """
                    SELECT id, customer_name, amount, status, updated_at
                    FROM public.orders
                    """
                )
                records = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}
                replica_row_count.set(len(records))

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
            else:
                replica_row_count.set(NAN)
                latest_row = None

            cursor.execute(
                "SELECT to_regclass('public.cdc_protected_deletes') IS NOT NULL"
            )
            audit_exists = bool(cursor.fetchone()[0])

            if audit_exists:
                cursor.execute(
                    """
                    SELECT
                      COUNT(*)::double precision,
                      COALESCE(EXTRACT(EPOCH FROM MAX(protected_at)), 0)::double precision
                    FROM public.cdc_protected_deletes
                    """
                )
                audit_count, latest_audit_timestamp = cursor.fetchone()
                protected_delete_events.set(float(audit_count or 0))
                last_protected_delete_timestamp_seconds.set(
                    float(latest_audit_timestamp or 0)
                )
            else:
                protected_delete_events.set(0)
                last_protected_delete_timestamp_seconds.set(0)

    if latest_row is None:
        end_to_end_latency_seconds.set(0)
        last_replica_write_timestamp_seconds.set(0)
    else:
        end_to_end_latency_seconds.set(float(latest_row[0] or 0))
        last_replica_write_timestamp_seconds.set(float(latest_row[1] or 0))

    return records


def collect_debezium_metrics():
    request = urllib.request.Request(DEBEZIUM_STATUS_URL)
    with urllib.request.urlopen(request, timeout=3) as response:
        status = json.load(response)

    connector_running = status.get("connector", {}).get("state") == "RUNNING"
    tasks = status.get("tasks", [])
    tasks_running = bool(tasks) and all(task.get("state") == "RUNNING" for task in tasks)

    debezium_connector_up.set(1 if connector_running else 0)
    debezium_task_up.set(1 if tasks_running else 0)


def collect_backup_metrics():
    completed_backups = [
        path
        for path in BACKUP_DIR.glob("replica_*.dump")
        if path.is_file() and path.stat().st_size > 0
    ]

    backup_files_total.set(len(completed_backups))

    if not completed_backups:
        backup_present.set(0)
        last_backup_timestamp_seconds.set(0)
        last_backup_age_seconds.set(NAN)
        return

    latest_backup = max(completed_backups, key=lambda path: path.stat().st_mtime)
    latest_timestamp = latest_backup.stat().st_mtime
    backup_present.set(1)
    last_backup_timestamp_seconds.set(latest_timestamp)
    last_backup_age_seconds.set(max(time.time() - latest_timestamp, 0))


def update_consistency_metrics(source_records, replica_records):
    if source_records is None or replica_records is None:
        row_count_difference.set(NAN)
        protected_replica_rows.set(NAN)
        active_rows_missing.set(NAN)
        active_rows_mismatched.set(NAN)
        return

    source_ids = set(source_records)
    replica_ids = set(replica_records)
    shared_ids = source_ids & replica_ids

    row_count_difference.set(abs(len(source_records) - len(replica_records)))
    protected_replica_rows.set(len(replica_ids - source_ids))
    active_rows_missing.set(len(source_ids - replica_ids))
    active_rows_mismatched.set(
        sum(
            1
            for row_id in shared_ids
            if source_records[row_id] != replica_records[row_id]
        )
    )


def collect_once():
    source_records = None
    replica_records = None

    try:
        source_records = collect_source_metrics()
    except Exception as error:
        source_database_up.set(0)
        source_orders_table_present.set(0)
        replication_slot_present.set(0)
        print(f"source metrics error: {error}", flush=True)

    try:
        replica_records = collect_replica_metrics()
    except Exception as error:
        replica_database_up.set(0)
        replica_orders_table_present.set(0)
        print(f"replica metrics error: {error}", flush=True)

    update_consistency_metrics(source_records, replica_records)

    try:
        collect_debezium_metrics()
    except Exception as error:
        debezium_connector_up.set(0)
        debezium_task_up.set(0)
        print(f"Debezium metrics error: {error}", flush=True)

    try:
        collect_backup_metrics()
    except Exception as error:
        backup_present.set(0)
        print(f"backup metrics error: {error}", flush=True)


if __name__ == "__main__":
    start_http_server(8000)
    print("CDC protection metrics exporter listening on port 8000", flush=True)

    while True:
        collect_once()
        time.sleep(POLL_INTERVAL_SECONDS)
