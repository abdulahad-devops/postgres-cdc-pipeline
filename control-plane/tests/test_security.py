import os

os.environ.setdefault("APP_SECRET", "test-secret-that-is-longer-than-thirty-two-characters")

from app.postgres_ops import validate_identifier
from app.security import hash_password, verify_password


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
