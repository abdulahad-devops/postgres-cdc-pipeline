# Apply the Delete-Protection and Backup Update

This update changes the replica from an exact mirror into a delete-protected archival replica:

- Source inserts and updates still reach the replica.
- Source deletes are recorded in `public.cdc_protected_deletes` and do not remove replica rows.
- Source and replica `orders` table presence is monitored separately.
- A backup is created immediately at startup and then every hour.
- Completed backups are kept in `./backups` for seven days by default.

## 1. Copy the update files

Extract this package into `D:\postgres-cdc-pipeline` and replace matching files.

## 2. Validate Docker Compose

```powershell
docker compose config --quiet
```

No output means the Compose configuration is valid.

## 3. Apply the audit table to the existing replica volume

Docker initialization SQL only runs automatically for a new volume. For the existing database, run:

```powershell
Get-Content .\replica-db\observability.sql -Raw | docker compose exec -T replica-postgres psql -v ON_ERROR_STOP=1 -U postgres -d replica_db
```

Apply full old-row capture to the existing source volume so delete audits can
include the deleted row details:

```powershell
Get-Content .\source-db\observability.sql -Raw | docker compose exec -T source-postgres psql -v ON_ERROR_STOP=1 -U postgres -d source_db
```

## 4. Rebuild and restart the changed services

```powershell
docker compose up -d --build --force-recreate sink cdc-metrics backup grafana
```

Wait 15 seconds:

```powershell
Start-Sleep -Seconds 15
```

## 5. Check service state

```powershell
docker compose ps -a
```

`sink`, `cdc-metrics`, `backup`, and `grafana` should be running. The normal one-shot services `redpanda-config` and `connector-init` can show `Exited (0)`.

## 6. Check protection and backup metrics

```powershell
curl.exe -s http://localhost:8000/metrics | Select-String -Pattern "cdc_source_orders_table_present|cdc_replica_orders_table_present|cdc_protected_delete_events_total|cdc_protected_replica_rows|cdc_active_source_rows_missing_from_replica|cdc_active_source_rows_mismatched_in_replica|cdc_backup_present|cdc_last_backup_age_seconds"
```

Healthy initial values include:

```text
cdc_source_orders_table_present 1
cdc_replica_orders_table_present 1
cdc_active_source_rows_missing_from_replica 0
cdc_active_source_rows_mismatched_in_replica 0
cdc_backup_present 1
```

## 7. Run the recovery and delete-protection test

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\recovery-test.ps1 -TimeoutSeconds 120
```

Expected final message:

```text
CDC recovery and delete-protection test passed.
```

## 8. Run a real backup restore test

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\backup-restore-test.ps1 -TimeoutSeconds 90
```

This restores the newest dump into the temporary database `cdc_backup_restore_test`, verifies its `orders` table, and removes only that temporary database.

## 9. Open Grafana

Open `http://localhost:3000` and navigate to:

```text
Dashboards > CDC > PostgreSQL CDC Pipeline
```

New panels include:

- Source Orders Table
- Replica Orders Table
- Protected Delete Events
- Rows Protected in Replica
- Active Rows Missing from Replica
- Active Rows Mismatched
- Replica Backup
- Latest Backup Age
- Protected Deletes by Source Table
- Protected Delete Audit Trail (Latest 100)

Do not drop the real source `orders` table just to demonstrate the panel. A real drop can interrupt the Debezium connector and requires schema/publication recovery. The table-presence panels are intended to detect that accident safely.

## Important backup limitation

The protected replica is not a complete substitute for backups because an accidental update still propagates to it. Hourly dump files provide restore points, but files stored in the same project folder do not protect against full computer or disk loss. Copy important verified dumps to another disk or remote object storage.
