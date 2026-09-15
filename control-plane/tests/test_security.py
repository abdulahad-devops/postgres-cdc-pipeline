import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("APP_SECRET", "test-secret-that-is-longer-than-thirty-two-characters")

from app.postgres_ops import validate_identifier
from app.security import ensure_database_host_allowed, hash_password, verify_password


def test_password_hash_round_trip():
    salt, digest = hash_password("a-secure-test-password")
    assert verify_password("a-secure-test-password", salt, digest)
    assert not verify_password("a-different-password", salt, digest)


def test_postgres_identifier_allowlist():
    assert validate_identifier("orders_2026") == "orders_2026"


def test_postgres_identifier_rejects_sql():
    try:
        validate_identifier('orders"; DROP TABLE users; --')
    except ValueError:
        pass
    else:
        raise AssertionError("unsafe identifier was accepted")


def test_exact_private_database_host_allowlist(monkeypatch):
    monkeypatch.delenv("ALLOW_PRIVATE_DATABASES", raising=False)
    monkeypatch.setenv("PRIVATE_DATABASE_HOST_ALLOWLIST", "approved-db.internal")
    ensure_database_host_allowed("APPROVED-DB.INTERNAL.")


def test_unlisted_private_database_host_is_rejected(monkeypatch):
    monkeypatch.delenv("ALLOW_PRIVATE_DATABASES", raising=False)
    monkeypatch.setenv("PRIVATE_DATABASE_HOST_ALLOWLIST", "approved-db.internal")
    monkeypatch.setattr(
        "app.security.socket.getaddrinfo",
        lambda *_: [(None, None, None, None, ("10.0.0.10", 0))],
    )
    with pytest.raises(HTTPException) as error:
        ensure_database_host_allowed("unapproved-db.internal")
    assert error.value.status_code == 422
