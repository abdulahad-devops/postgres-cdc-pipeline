import ipaddress
import logging
import os
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, EmailStr, Field
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg.errors import UniqueViolation

from .cdc import apply_connector, connector_status, start_consumer
from .database import db, migrate
from .postgres_ops import (
    delete_source_row,
    discover_tables,
    insert_source_row,
    read_source_rows,
    test_connection,
    update_source_row,
)
from .secrets_store import put_connection_secret
from .security import (
    COOKIE_SECURE,
    csrf_token,
    current_identity,
    ensure_security_config,
    hash_password,
    issue_session,
    require_csrf,
    verify_password,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
ALLOW_PRIVATE_DATABASES = os.getenv("ALLOW_PRIVATE_DATABASES", "false").lower() == "true"


class RegisterBody(BaseModel):
    organization_name: str = Field(min_length=2, max_length=80)
    email: EmailStr
    password: str = Field(min_length=12, max_length=200)


class LoginBody(BaseModel):
    email: EmailStr
    password: str


class ConnectionBody(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    host: str = Field(min_length=3, max_length=253)
    port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = Field(min_length=1, max_length=63)
    database_user: str = Field(min_length=1, max_length=63)
    password: str = Field(min_length=1, max_length=500)
    sslmode: str = Field(default="require", pattern="^(require|verify-ca|verify-full)$")


class TableRef(BaseModel):
    schema_name: str
    table_name: str


class SelectTablesBody(BaseModel):
    tables: list[TableRef] = Field(min_length=1, max_length=100)


class InsertBody(BaseModel):
    values: dict[str, Any]


class UpdateBody(BaseModel):
    primary_key: dict[str, Any]
    values: dict[str, Any]


class DeleteBody(BaseModel):
    primary_key: dict[str, Any]


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_security_config()
    migrate()
    stop, thread = start_consumer()
    yield
    stop.set()
    thread.join(timeout=5)


app = FastAPI(title="CDC Control Plane", version="0.1.0", lifespan=lifespan)
allowed_hosts = [item.strip() for item in os.getenv("ALLOWED_HOSTS", "*").split(",") if item.strip()]
app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts or ["*"])
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")


def identity(request: Request) -> tuple[UUID, UUID]:
    return current_identity(request)


def mutation_identity(request: Request) -> tuple[UUID, UUID]:
    user_id, tenant_id = current_identity(request)
    require_csrf(request, user_id, tenant_id)
    return user_id, tenant_id


def owned_connection(connection_id: UUID, tenant_id: UUID) -> dict[str, Any]:
    with db() as control:
        row = control.execute(
            "SELECT * FROM source_connections WHERE id = %s AND tenant_id = %s",
            (connection_id, tenant_id),
        ).fetchone()
    if not row:
        raise HTTPException(404, "Connection not found")
    return row


def selected_table(connection_id: UUID, tenant_id: UUID, schema_name: str, table_name: str):
    connection = owned_connection(connection_id, tenant_id)
    with db() as control:
        selected = control.execute(
            """
            SELECT primary_key_columns FROM selected_tables
            WHERE connection_id = %s AND schema_name = %s AND table_name = %s
            """,
            (connection_id, schema_name, table_name),
        ).fetchone()
    if not selected:
        raise HTTPException(403, "This table is not enabled for the portal")
    return connection, selected


def ensure_public_database_host(host: str) -> None:
    if ALLOW_PRIVATE_DATABASES:
        return
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None)}
    except socket.gaierror as exc:
        raise HTTPException(422, "Database hostname could not be resolved") from exc
    if not addresses:
        raise HTTPException(422, "Database hostname could not be resolved")
    for raw in addresses:
        ip = ipaddress.ip_address(raw)
        if not ip.is_global:
            raise HTTPException(
                422,
                "Private, loopback and link-local database addresses are disabled for this deployment",
            )


def safe_database_error(exc: Exception) -> HTTPException:
    log.warning("Customer database operation failed: %s", type(exc).__name__)
    return HTTPException(502, f"PostgreSQL operation failed ({type(exc).__name__})")


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    with db() as control:
        control.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/api/register")
def register(body: RegisterBody, response: Response):
    tenant_id, user_id = uuid4(), uuid4()
    salt, digest = hash_password(body.password)
    try:
        with db() as control:
            control.execute(
                "INSERT INTO tenants (id, name) VALUES (%s, %s)",
                (tenant_id, body.organization_name.strip()),
            )
            control.execute(
                """
                INSERT INTO users (id, tenant_id, email, password_salt, password_hash)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, tenant_id, body.email.lower(), salt, digest),
            )
            control.commit()
    except UniqueViolation as exc:
        raise HTTPException(409, "An account with this email already exists") from exc
    response.set_cookie(
        "cdc_session", issue_session(user_id, tenant_id), httponly=True,
        secure=COOKIE_SECURE, samesite="lax", max_age=28800,
    )
    return {"email": body.email.lower(), "tenant_id": tenant_id, "csrf_token": csrf_token(user_id, tenant_id)}


@app.post("/api/login")
def login(body: LoginBody, response: Response):
    with db() as control:
        user = control.execute(
            "SELECT id, tenant_id, email, password_salt, password_hash FROM users WHERE email = %s",
            (body.email.lower(),),
        ).fetchone()
    if not user or not verify_password(body.password, bytes(user["password_salt"]), bytes(user["password_hash"])):
        raise HTTPException(401, "Invalid email or password")
    response.set_cookie(
        "cdc_session", issue_session(user["id"], user["tenant_id"]), httponly=True,
        secure=COOKIE_SECURE, samesite="lax", max_age=28800,
    )
    return {
        "email": user["email"], "tenant_id": user["tenant_id"],
        "csrf_token": csrf_token(user["id"], user["tenant_id"]),
    }


@app.post("/api/logout")
def logout(response: Response, _: tuple[UUID, UUID] = Depends(mutation_identity)):
    response.delete_cookie("cdc_session")
    return {"ok": True}


@app.get("/api/me")
def me(ids: tuple[UUID, UUID] = Depends(identity)):
    user_id, tenant_id = ids
    with db() as control:
        row = control.execute(
            """
            SELECT u.email, t.name AS organization_name
            FROM users u JOIN tenants t ON t.id = u.tenant_id
            WHERE u.id = %s AND u.tenant_id = %s
            """,
            (user_id, tenant_id),
        ).fetchone()
    if not row:
        raise HTTPException(401, "Account not found")
    return {**row, "tenant_id": tenant_id, "csrf_token": csrf_token(user_id, tenant_id)}


@app.get("/api/connections")
def list_connections(ids: tuple[UUID, UUID] = Depends(identity)):
    _, tenant_id = ids
    with db() as control:
        rows = control.execute(
            """
            SELECT id, name, host, port, database_name, database_user, sslmode,
                   connector_name, status, last_error, created_at, updated_at
            FROM source_connections WHERE tenant_id = %s ORDER BY created_at DESC
            """,
            (tenant_id,),
        ).fetchall()
    return jsonable_encoder(rows)


@app.post("/api/connections")
def create_connection(body: ConnectionBody, ids: tuple[UUID, UUID] = Depends(mutation_identity)):
    _, tenant_id = ids
    ensure_public_database_host(body.host)
    draft = body.model_dump(exclude={"password"})
    try:
        probe = test_connection(draft, body.password)
    except Exception as exc:
        raise safe_database_error(exc) from exc

    connection_id = uuid4()
    stem = f"tenant_{tenant_id.hex[:8]}_{connection_id.hex[:8]}"
    secret_ref = put_connection_secret(str(tenant_id), str(connection_id), {"password": body.password})
    try:
        with db() as control:
            control.execute(
                """
                INSERT INTO source_connections (
                    id, tenant_id, name, host, port, database_name, database_user, sslmode,
                    secret_ref, connector_name, topic_prefix, publication_name, slot_name, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'connected')
                """,
                (
                    connection_id, tenant_id, body.name.strip(), body.host, body.port,
                    body.database_name, body.database_user, body.sslmode, secret_ref,
                    f"{stem}-connector", stem, f"{stem}_publication", f"{stem}_slot",
                ),
            )
            control.commit()
    except UniqueViolation as exc:
        raise HTTPException(409, "A connection with this name already exists") from exc
    return {"id": connection_id, "status": "connected", "server": jsonable_encoder(probe)}


@app.get("/api/connections/{connection_id}/tables")
def tables(connection_id: UUID, ids: tuple[UUID, UUID] = Depends(identity)):
    _, tenant_id = ids
    connection = owned_connection(connection_id, tenant_id)
    try:
        discovered = discover_tables(connection)
    except Exception as exc:
        raise safe_database_error(exc) from exc
    with db() as control:
        selected = {
            (row["schema_name"], row["table_name"])
            for row in control.execute(
                "SELECT schema_name, table_name FROM selected_tables WHERE connection_id = %s",
                (connection_id,),
            ).fetchall()
        }
    for row in discovered:
        row["selected"] = (row["schema_name"], row["table_name"]) in selected
    return jsonable_encoder(discovered)


@app.put("/api/connections/{connection_id}/tables")
def select_tables(
    connection_id: UUID,
    body: SelectTablesBody,
    ids: tuple[UUID, UUID] = Depends(mutation_identity),
):
    _, tenant_id = ids
    connection = owned_connection(connection_id, tenant_id)
    try:
        available = {
            (row["schema_name"], row["table_name"]): row
            for row in discover_tables(connection)
        }
    except Exception as exc:
        raise safe_database_error(exc) from exc

    chosen = []
    for requested in body.tables:
        row = available.get((requested.schema_name, requested.table_name))
        if not row:
            raise HTTPException(422, f"Table not found: {requested.schema_name}.{requested.table_name}")
        if not row["primary_key_columns"]:
            raise HTTPException(422, f"Primary key required: {requested.schema_name}.{requested.table_name}")
        chosen.append(row)

    with db() as control:
        control.execute("DELETE FROM selected_tables WHERE connection_id = %s", (connection_id,))
        for row in chosen:
            control.execute(
                """
                INSERT INTO selected_tables (connection_id, schema_name, table_name, primary_key_columns)
                VALUES (%s, %s, %s, %s)
                """,
                (connection_id, row["schema_name"], row["table_name"], row["primary_key_columns"]),
            )
        control.commit()

    try:
        apply_connector(connection, chosen)
        state, error = "running", None
    except (httpx.HTTPError, ValueError) as exc:
        state, error = "connector_error", str(exc)[:1000]
    with db() as control:
        control.execute(
            "UPDATE source_connections SET status=%s, last_error=%s, updated_at=now() WHERE id=%s",
            (state, error, connection_id),
        )
        control.commit()
    if error:
        raise HTTPException(502, "Tables saved, but Debezium connector configuration failed")
    return {"status": state, "tables": jsonable_encoder(chosen)}


@app.get("/api/connections/{connection_id}/status")
def connection_status(connection_id: UUID, ids: tuple[UUID, UUID] = Depends(identity)):
    _, tenant_id = ids
    connection = owned_connection(connection_id, tenant_id)
    with db() as control:
        counts = control.execute(
            """
            SELECT count(*) FILTER (WHERE NOT is_deleted) AS active_records,
                   count(*) FILTER (WHERE is_deleted) AS protected_deletes,
                   max(replicated_at) AS latest_replication
            FROM replica_records
            WHERE connection_id = %s AND tenant_id = %s
            """,
            (connection_id, tenant_id),
        ).fetchone()
    return jsonable_encoder({**connector_status(connection["connector_name"]), **counts})


@app.get("/api/connections/{connection_id}/rows/{schema_name}/{table_name}")
def rows(
    connection_id: UUID,
    schema_name: str,
    table_name: str,
    view: str = Query(default="source", pattern="^(source|replica)$"),
    limit: int = Query(default=100, ge=1, le=500),
    ids: tuple[UUID, UUID] = Depends(identity),
):
    _, tenant_id = ids
    connection, _ = selected_table(connection_id, tenant_id, schema_name, table_name)
    if view == "replica":
        with db() as control:
            mirror = control.execute(
                """
                SELECT record_key, record_data, is_deleted, source_lsn, replicated_at, deleted_at
                FROM replica_records
                WHERE connection_id=%s AND tenant_id=%s AND schema_name=%s AND table_name=%s
                ORDER BY replicated_at DESC LIMIT %s
                """,
                (connection_id, tenant_id, schema_name, table_name, limit),
            ).fetchall()
        return jsonable_encoder({"rows": mirror})
    try:
        return jsonable_encoder(read_source_rows(connection, schema_name, table_name, limit))
    except Exception as exc:
        raise safe_database_error(exc) from exc


@app.post("/api/connections/{connection_id}/rows/{schema_name}/{table_name}")
def insert_row(
    connection_id: UUID, schema_name: str, table_name: str, body: InsertBody,
    ids: tuple[UUID, UUID] = Depends(mutation_identity),
):
    _, tenant_id = ids
    connection, _ = selected_table(connection_id, tenant_id, schema_name, table_name)
    try:
        return jsonable_encoder(insert_source_row(connection, schema_name, table_name, body.values))
    except Exception as exc:
        raise safe_database_error(exc) from exc


@app.patch("/api/connections/{connection_id}/rows/{schema_name}/{table_name}")
def update_row(
    connection_id: UUID, schema_name: str, table_name: str, body: UpdateBody,
    ids: tuple[UUID, UUID] = Depends(mutation_identity),
):
    _, tenant_id = ids
    connection, selected = selected_table(connection_id, tenant_id, schema_name, table_name)
    try:
        return jsonable_encoder(
            update_source_row(
                connection, schema_name, table_name, body.values,
                body.primary_key, selected["primary_key_columns"],
            )
        )
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise safe_database_error(exc) from exc


@app.delete("/api/connections/{connection_id}/rows/{schema_name}/{table_name}")
def delete_row(
    connection_id: UUID, schema_name: str, table_name: str, body: DeleteBody,
    ids: tuple[UUID, UUID] = Depends(mutation_identity),
):
    _, tenant_id = ids
    connection, selected = selected_table(connection_id, tenant_id, schema_name, table_name)
    try:
        deleted = delete_source_row(
            connection, schema_name, table_name,
            body.primary_key, selected["primary_key_columns"],
        )
        return {"deleted_from_source": jsonable_encoder(deleted), "replica_policy": "preserve_and_audit"}
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise safe_database_error(exc) from exc


@app.get("/api/connections/{connection_id}/delete-audit")
def delete_audit(
    connection_id: UUID,
    limit: int = Query(default=100, ge=1, le=500),
    ids: tuple[UUID, UUID] = Depends(identity),
):
    _, tenant_id = ids
    owned_connection(connection_id, tenant_id)
    with db() as control:
        rows = control.execute(
            """
            SELECT schema_name, table_name, record_key, record_data, source_lsn, protected_at
            FROM delete_audit WHERE connection_id=%s AND tenant_id=%s
            ORDER BY protected_at DESC LIMIT %s
            """,
            (connection_id, tenant_id, limit),
        ).fetchall()
    return jsonable_encoder(rows)
