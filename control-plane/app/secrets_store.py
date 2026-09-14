import base64
import hashlib
import json
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

BACKEND = os.getenv("SECRETS_BACKEND", "fernet").lower()
ENCRYPTION_KEY = os.getenv("APP_ENCRYPTION_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_SECRET_PREFIX = os.getenv("AWS_SECRET_PREFIX", "postgres-cdc")


def _fernet() -> Fernet:
    if len(ENCRYPTION_KEY) < 32:
        raise RuntimeError("APP_ENCRYPTION_KEY must contain at least 32 characters")
    key = base64.urlsafe_b64encode(hashlib.sha256(ENCRYPTION_KEY.encode()).digest())
    return Fernet(key)


def put_connection_secret(tenant_id: str, connection_id: str, payload: dict[str, Any]) -> str:
    if BACKEND == "aws":
        name = f"{AWS_SECRET_PREFIX}/{tenant_id}/{connection_id}"
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        value = json.dumps(payload)
        try:
            client.create_secret(Name=name, SecretString=value)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ResourceExistsException":
                raise
            client.put_secret_value(SecretId=name, SecretString=value)
        return f"aws:{name}"

    token = _fernet().encrypt(json.dumps(payload).encode()).decode()
    return f"fernet:{token}"


def get_connection_secret(reference: str) -> dict[str, Any]:
    kind, value = reference.split(":", 1)
    if kind == "aws":
        response = boto3.client("secretsmanager", region_name=AWS_REGION).get_secret_value(SecretId=value)
        return json.loads(response["SecretString"])
    if kind == "fernet":
        return json.loads(_fernet().decrypt(value.encode()).decode())
    raise RuntimeError("Unsupported secret reference")
