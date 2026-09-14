import hashlib
import json
import logging
import os
import threading
import time
from typing import Any

import httpx
from kafka import KafkaConsumer
from psycopg.types.json import Jsonb
from prometheus_client import Counter

from .database import db
from .secrets_store import get_connection_secret

log = logging.getLogger(__name__)
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

EVENTS_RECEIVED = Counter("saas_cdc_events_received_total", "Tenant CDC events received")
EVENTS_MIRRORED = Counter("saas_cdc_events_mirrored_total", "Tenant CDC events written to mirror")
EVENT_ERRORS = Counter("saas_cdc_event_errors_total", "Tenant CDC event processing errors")


def connector_config(connection: dict[str, Any], tables: list[dict[str, Any]]) -> dict[str, str]:
    password = get_connection_secret(connection["secret_ref"])["password"]
    include = ",".join(f'{row["schema_name"]}.{row["table_name"]}' for row in tables)
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": connection["host"],
        "database.port": str(connection["port"]),
        "database.user": connection["database_user"],
        "database.password": password,
        "database.dbname": connection["database_name"],
        "database.sslmode": connection["sslmode"],
        "topic.prefix": connection["topic_prefix"],
        "plugin.name": "pgoutput",
        "slot.name": connection["slot_name"],
        "publication.name": connection["publication_name"],
        "publication.autocreate.mode": "filtered",
        "table.include.list": include,
        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


def apply_connector(connection: dict[str, Any], tables: list[dict[str, Any]]) -> dict[str, Any]:
    if not tables:
        raise ValueError("Select at least one table")
    url = f'{DEBEZIUM_URL}/connectors/{connection["connector_name"]}/config'
    response = httpx.put(url, json=connector_config(connection, tables), timeout=20)
    response.raise_for_status()
    return response.json()


def connector_status(name: str) -> dict[str, Any]:
    try:
        response = httpx.get(f"{DEBEZIUM_URL}/connectors/{name}/status", timeout=5)
        if response.status_code == 404:
            return {"connector": "NOT_CONFIGURED", "task": "NOT_CONFIGURED"}
        response.raise_for_status()
        data = response.json()
        tasks = data.get("tasks", [])
        return {
            "connector": data.get("connector", {}).get("state", "UNKNOWN"),
            "task": tasks[0].get("state", "UNKNOWN") if tasks else "NO_TASK",
        }
    except Exception as exc:
        return {"connector": "UNREACHABLE", "task": "UNREACHABLE", "detail": str(exc)}


def _payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    nested = value.get("payload")
    return nested if isinstance(nested, dict) else value


def _key(value: Any) -> dict[str, Any]:
    payload = _payload(value)
    return payload if isinstance(payload, dict) else {}


def _mirror(message) -> None:
    value = _payload(message.value)
    operation = value.get("op")
    if operation not in {"c", "r", "u", "d"}:
        return

    prefix, schema_name, table_name = message.topic.split(".", 2)
    event_id = f"{message.topic}:{message.partition}:{message.offset}"
    record_key = _key(message.key)
    record_data = value.get("after") if operation != "d" else value.get("before")
    record_data = record_data or {}
    if not record_key:
        with db() as control:
            selected = control.execute(
                """
                SELECT st.primary_key_columns
                FROM selected_tables st
                JOIN source_connections sc ON sc.id = st.connection_id
                WHERE sc.topic_prefix = %s AND st.schema_name = %s AND st.table_name = %s
                """,
                (prefix, schema_name, table_name),
            ).fetchone()
        if selected:
            record_key = {name: record_data.get(name) for name in selected["primary_key_columns"]}

    canonical_key = json.dumps(record_key, sort_keys=True, separators=(",", ":"), default=str)
    key_hash = hashlib.sha256(canonical_key.encode()).hexdigest()
    source_lsn = str((value.get("source") or {}).get("lsn") or "")

    with db() as control:
        connection = control.execute(
            "SELECT id, tenant_id FROM source_connections WHERE topic_prefix = %s",
            (prefix,),
        ).fetchone()
        if not connection:
            return
        params = (
            connection["id"], connection["tenant_id"], schema_name, table_name, key_hash,
            Jsonb(record_key), Jsonb(record_data), operation == "d", source_lsn, event_id,
            operation == "d",
        )
        control.execute(
            """
            INSERT INTO replica_records (
                connection_id, tenant_id, schema_name, table_name, key_hash,
                record_key, record_data, is_deleted, source_lsn, last_event_id, deleted_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END)
            ON CONFLICT (connection_id, schema_name, table_name, key_hash)
            DO UPDATE SET
                record_key = EXCLUDED.record_key,
                record_data = CASE
                    WHEN EXCLUDED.is_deleted AND EXCLUDED.record_data = '{}'::jsonb
                    THEN replica_records.record_data ELSE EXCLUDED.record_data END,
                is_deleted = EXCLUDED.is_deleted,
                source_lsn = EXCLUDED.source_lsn,
                last_event_id = EXCLUDED.last_event_id,
                replicated_at = now(),
                deleted_at = CASE WHEN EXCLUDED.is_deleted THEN now() ELSE NULL END
            WHERE replica_records.last_event_id <> EXCLUDED.last_event_id
            """,
            params,
        )
        if operation == "d":
            control.execute(
                """
                INSERT INTO delete_audit (
                    event_id, connection_id, tenant_id, schema_name, table_name,
                    record_key, record_data, source_lsn
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event_id, connection["id"], connection["tenant_id"], schema_name, table_name,
                    Jsonb(record_key), Jsonb(record_data), source_lsn,
                ),
            )
        control.commit()
    EVENTS_MIRRORED.inc()


def run_consumer(stop: threading.Event) -> None:
    while not stop.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                group_id="saas-generic-replica-v1",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                key_deserializer=lambda raw: json.loads(raw.decode()) if raw else {},
                value_deserializer=lambda raw: json.loads(raw.decode()) if raw else {},
                consumer_timeout_ms=1000,
            )
            consumer.subscribe(pattern=r"^tenant_[^.]+\.[^.]+\.[^.]+$")
            while not stop.is_set():
                batches = consumer.poll(timeout_ms=1000, max_records=100)
                for messages in batches.values():
                    for message in messages:
                        EVENTS_RECEIVED.inc()
                        try:
                            _mirror(message)
                        except Exception:
                            EVENT_ERRORS.inc()
                            log.exception("Could not mirror event %s:%s", message.topic, message.offset)
                            raise
                if batches:
                    consumer.commit()
        except Exception:
            EVENT_ERRORS.inc()
            log.exception("Tenant CDC consumer disconnected; retrying")
            stop.wait(5)
        finally:
            if consumer is not None:
                consumer.close()


def start_consumer() -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()
    thread = threading.Thread(target=run_consumer, args=(stop,), daemon=True, name="tenant-cdc-consumer")
    thread.start()
    return stop, thread
