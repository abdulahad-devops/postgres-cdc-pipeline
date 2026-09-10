param(
    [int]$TimeoutSeconds = 90
)

$ErrorActionPreference = "Stop"
$testDatabase = "cdc_backup_restore_test"

Write-Host "Requesting an immediate replica backup by restarting the backup service..."
$requestTime = (Get-Date).ToUniversalTime()

docker compose restart backup
if ($LASTEXITCODE -ne 0) {
    throw "Could not restart the backup service."
}

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$latestBackup = $null

do {
    $backups = @(
        Get-ChildItem .\backups\replica_*.dump -ErrorAction SilentlyContinue |
            Where-Object { $_.Length -gt 0 } |
            Sort-Object LastWriteTimeUtc -Descending
    )

    if ($backups.Count -gt 0 -and $backups[0].LastWriteTimeUtc -ge $requestTime.AddSeconds(-1)) {
        $latestBackup = $backups[0]
        break
    }

    Start-Sleep -Seconds 2
} while ((Get-Date) -lt $deadline)

if ($null -eq $latestBackup) {
    throw "A new completed backup did not appear within $TimeoutSeconds seconds."
}

$containerBackupPath = "/backups/$($latestBackup.Name)"
Write-Host "Testing backup: $($latestBackup.FullName)"

try {
    docker compose exec -T backup dropdb `
        -h replica-postgres `
        -U postgres `
        --if-exists `
        --force `
        $testDatabase

    if ($LASTEXITCODE -ne 0) {
        throw "Could not prepare the temporary restore database."
    }

    docker compose exec -T backup createdb `
        -h replica-postgres `
        -U postgres `
        $testDatabase

    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the temporary restore database."
    }

    docker compose exec -T backup pg_restore `
        -h replica-postgres `
        -U postgres `
        -d $testDatabase `
        --no-owner `
        --no-privileges `
        $containerBackupPath

    if ($LASTEXITCODE -ne 0) {
        throw "The backup could not be restored."
    }

    $restoredRows = docker compose exec -T replica-postgres psql `
        -X `
        -qAt `
        -v ON_ERROR_STOP=1 `
        -U postgres `
        -d $testDatabase `
        -c "SELECT COUNT(*) FROM public.orders;"

    if ($LASTEXITCODE -ne 0) {
        throw "Could not query the restored orders table."
    }

    Write-Host "Backup restore test passed. Restored orders: $restoredRows"
}
finally {
    Write-Host "Removing temporary restore database $testDatabase..."
    docker compose exec -T backup dropdb `
        -h replica-postgres `
        -U postgres `
        --if-exists `
        --force `
        $testDatabase | Out-Host
}
