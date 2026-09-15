# Multi-tenant CDC portal MVP

This additive MVP turns the existing single-table demo into a tenant-aware portal without removing the original demo. A user can create an account, connect a reachable cloud PostgreSQL database, select tables with primary keys, run safe row-level CRUD, and compare the source with a protected JSONB replica.

## What this version includes

- Tenant-scoped users, connections, table selections, replica records, and delete audit.
- Customer credentials stored in AWS Secrets Manager, or encrypted with Fernet for a first test.
- A unique Debezium connector, logical replication slot, publication, and topic prefix for every connection.
- Table allowlisting. The API never accepts arbitrary SQL.
- Identifier validation and parameterized values/primary keys.
- Source, protected-replica, connector/task health, and delete counters in the portal.
- Prometheus metrics and an automatically provisioned SaaS PostgreSQL CDC Grafana dashboard.
- Source deletes are preserved in replica_records and appended to delete_audit.

This is an MVP, not yet a public self-service production platform. Before untrusted customers are onboarded, add email verification, password reset, rate limits, billing/quotas, an egress proxy, per-tenant Kafka ACLs, audit export/retention, and a formal security review.

## Architecture

1. The browser talks only to control-plane.
2. The control plane stores product metadata in control-postgres.
3. A password is retrieved from Secrets Manager only when a source operation or connector update needs it.
4. Debezium creates PostgreSQL CDC events in a tenant-specific Redpanda topic.
5. The generic consumer writes the latest row to the JSONB replica. Deletes mark a row as deleted and retain its last contents.
6. Prometheus scrapes the control plane; Grafana displays aggregate processing metrics.

Grafana remains the operator dashboard. Customers use the control-plane portal rather than receiving Grafana admin credentials.

## Customer database requirements

The MVP accepts cloud PostgreSQL/RDS, not a PostgreSQL instance running on a customer's laptop.

- PostgreSQL logical replication must be enabled (wal_level=logical; on RDS use a parameter group with rds.logical_replication=1 and reboot).
- The database must be reachable from the EC2 service. Prefer an RDS security-group rule whose source is the EC2 security group.
- For a private RDS endpoint, keep `ALLOW_PRIVATE_DATABASES=false` and add only the trusted endpoint to `PRIVATE_DATABASE_HOST_ALLOWLIST`. Use comma-separated exact hostnames when onboarding more than one approved private database.
- SSL is required by the portal.
- Every selected table must have a primary key.
- Prefer `REPLICA IDENTITY FULL` on selected tables when complete PostgreSQL before-images are required. The protected replica still retains its last mirrored row on deletes.
- Use a dedicated service role. Never enter an AWS master or PostgreSQL superuser account into the portal.
- The Debezium role needs LOGIN, logical replication permission, CONNECT, schema USAGE, table SELECT, and enough ownership/CREATE privilege to create and maintain the filtered publication.
- Add INSERT, UPDATE, DELETE, and sequence privileges only because this selected MVP includes CRUD.

Example for ordinary PostgreSQL (adapt names and tables as the database owner):

```sql
CREATE ROLE cdc_portal LOGIN REPLICATION PASSWORD 'use-a-secret-manager-value';
GRANT CONNECT, CREATE ON DATABASE app_db TO cdc_portal;
GRANT USAGE ON SCHEMA public TO cdc_portal;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.orders TO cdc_portal;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO cdc_portal;
```

On Amazon RDS, grant the managed replication role instead of PostgreSQL superuser:

```sql
GRANT rds_replication TO cdc_portal;
```

PostgreSQL publication ownership rules still apply. For a serious deployment, use a shared owner role for the selected tables, or have a DBA create/manage publications through a reviewed onboarding workflow.

## First deployment on the existing EC2 host

Run these commands inside the repository on EC2 after this branch is merged or checked out:

```bash
cd ~/postgres-cdc-pipeline
umask 077

CONTROL_POSTGRES_PASSWORD="$(openssl rand -hex 32)"
APP_SECRET="$(openssl rand -hex 48)"
APP_ENCRYPTION_KEY="$(openssl rand -hex 48)"

{
  echo "CONTROL_POSTGRES_PASSWORD=$CONTROL_POSTGRES_PASSWORD"
  echo "APP_SECRET=$APP_SECRET"
  echo "APP_ENCRYPTION_KEY=$APP_ENCRYPTION_KEY"
  echo "SECRETS_BACKEND=fernet"
  echo "AWS_REGION=ap-south-1"
  echo "AWS_SECRET_PREFIX=postgres-cdc"
  echo "COOKIE_SECURE=true"
  echo "ALLOWED_HOSTS=*"
  echo "ALLOW_PRIVATE_DATABASES=false"
  echo "PRIVATE_DATABASE_HOST_ALLOWLIST=your-db.cluster-id.ap-south-1.rds.amazonaws.com"
} > .env.saas

unset CONTROL_POSTGRES_PASSWORD APP_SECRET APP_ENCRYPTION_KEY

docker compose \
  --env-file .env.production \
  --env-file .env.saas \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.saas.yml \
  up -d --build
```

Validate it locally on EC2:

```bash
curl -s http://127.0.0.1:8080/health
docker compose \
  --env-file .env.production \
  --env-file .env.saas \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.saas.yml \
  ps -a
```

For a temporary review URL, point the tunnel to the product portal (port 8080), not Grafana:

```bash
cloudflared tunnel --url http://127.0.0.1:8080
```

Quick Tunnel URLs change when restarted and may be flagged by browser reputation systems. Use a named Cloudflare Tunnel plus a controlled domain before sending this to real customers.

## Move credentials to AWS Secrets Manager

For the first smoke test, SECRETS_BACKEND=fernet is acceptable: passwords are encrypted before entering the control database. For AWS production, attach an IAM role to EC2 with narrowly scoped access to:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "secretsmanager:CreateSecret",
      "secretsmanager:PutSecretValue",
      "secretsmanager:GetSecretValue"
    ],
    "Resource": "arn:aws:secretsmanager:ap-south-1:*:secret:postgres-cdc/*"
  }]
}
```

Then change SECRETS_BACKEND=aws in .env.saas and recreate the control plane:

```bash
docker compose \
  --env-file .env.production \
  --env-file .env.saas \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.saas.yml \
  up -d --build control-plane
```

Do not commit .env.saas, .env.production, customer passwords, database dumps, tunnel credentials, or PEM keys.

## Smoke-test workflow

1. Open the port-8080 tunnel URL and create a workspace.
2. Connect a test RDS/PostgreSQL database with the dedicated role.
3. Select one table that has a primary key.
4. Wait until connector and task show RUNNING.
5. Open the table and insert a test row.
6. Switch to Protected replica; the row should appear after CDC processing.
7. Update the source row and verify the mirror changes.
8. Delete the row. It should disappear from Source and remain in Protected replica with Protected.
9. Open Grafana internally and select the SaaS PostgreSQL CDC dashboard.

## Current isolation boundary

All product queries include tenant_id and validate ownership before returning connection/table/row metadata. The generic replica uses (connection_id, schema, table, key_hash) as its row key. Topic, connector, slot, and publication identifiers are unique per connection.

The Redpanda cluster, Debezium worker, control database, and consumer group are shared infrastructure in this MVP. Stronger enterprise isolation would use per-tenant Kafka ACLs or separate data planes.
