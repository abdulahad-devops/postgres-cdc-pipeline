param(
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

if ($TimeoutSeconds -lt 10) {
    throw "TimeoutSeconds must be at least 10."
}

function Invoke-PsqlScalar {
    param(
        [string]$Service,
        [string]$Database,
        [string]$Sql
    )

    $result = docker compose exec -T $Service psql `
        -X `
        -qAt `
        -v ON_ERROR_STOP=1 `
        -U postgres `
        -d $Database `
        -c $Sql

    if ($LASTEXITCODE -ne 0) {
        throw "SQL command failed for service $Service."
    }

    $value = $result |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -Last 1

    return "$value".Trim()
}

$testName = "Recovery-$((Get-Date).ToString('yyyyMMddHHmmss'))"
$testId = $null
$sinkStopped = $false
$testPassed = $false

try {
    Write-Host "Stopping the sink consumer..."
    docker compose stop sink

    if ($LASTEXITCODE -ne 0) {
        throw "Could not stop the sink service."
    }

    $sinkStopped = $true

    $insertSql = @"
INSERT INTO public.orders (customer_name, amount, status)
VALUES ('$testName', 2000.00, 'Pending')
RETURNING id;
"@

    $testId = Invoke-PsqlScalar `
        -Service "source-postgres" `
        -Database "source_db" `
        -Sql $insertSql

    if ($testId -notmatch '^\d+$') {
        throw "Could not read the inserted recovery-test ID. Received: $testId"
    }

    Write-Host "Inserted source order ID $testId while the sink is stopped."
    Start-Sleep -Seconds 3

    $replicaCountBeforeRestart = [int](Invoke-PsqlScalar `
        -Service "replica-postgres" `
        -Database "replica_db" `
        -Sql "SELECT COUNT(*) FROM public.orders WHERE id = $testId;")

    if ($replicaCountBeforeRestart -ne 0) {
        throw "The test row reached the replica before the sink was restarted."
    }

    Write-Host "Restarting the sink consumer..."
    docker compose start sink

    if ($LASTEXITCODE -ne 0) {
        throw "Could not restart the sink service."
    }

    $sinkStopped = $false
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $replicaCount = 0

    do {
        $replicaCount = [int](Invoke-PsqlScalar `
            -Service "replica-postgres" `
            -Database "replica_db" `
            -Sql "SELECT COUNT(*) FROM public.orders WHERE id = $testId;")

        Write-Host "Replica copies for order $testId`: $replicaCount"

        if ($replicaCount -eq 1) {
            break
        }

        if ($replicaCount -gt 1) {
            throw "Duplicate rows detected for order $testId."
        }

        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    if ($replicaCount -ne 1) {
        throw "The replica did not recover order $testId within $TimeoutSeconds seconds."
    }

    $sourceChecksum = Invoke-PsqlScalar `
        -Service "source-postgres" `
        -Database "source_db" `
        -Sql "SELECT MD5(CONCAT_WS('|', id, customer_name, amount, status, updated_at)) FROM public.orders WHERE id = $testId;"

    $replicaChecksum = Invoke-PsqlScalar `
        -Service "replica-postgres" `
        -Database "replica_db" `
        -Sql "SELECT MD5(CONCAT_WS('|', id, customer_name, amount, status, updated_at)) FROM public.orders WHERE id = $testId;"

    if ([string]::IsNullOrWhiteSpace($sourceChecksum) -or $sourceChecksum -ne $replicaChecksum) {
        throw "Recovered row content does not match the source row."
    }

    $testPassed = $true
    Write-Host "CDC recovery test passed: no event loss, no duplicate row, and matching content."
}
finally {
    if ($sinkStopped) {
        Write-Host "Ensuring the sink service is running..."
        docker compose start sink | Out-Host
        $sinkStopped = $false
    }

    if ($null -ne $testId -and "$testId" -match '^\d+$') {
        Write-Host "Removing recovery-test order $testId..."

        $null = Invoke-PsqlScalar `
            -Service "source-postgres" `
            -Database "source_db" `
            -Sql "WITH deleted AS (DELETE FROM public.orders WHERE id = $testId RETURNING 1) SELECT COUNT(*) FROM deleted;"

        $cleanupDeadline = (Get-Date).AddSeconds($TimeoutSeconds)

        do {
            $remainingReplicaRows = [int](Invoke-PsqlScalar `
                -Service "replica-postgres" `
                -Database "replica_db" `
                -Sql "SELECT COUNT(*) FROM public.orders WHERE id = $testId;")

            if ($remainingReplicaRows -eq 0) {
                break
            }

            Start-Sleep -Seconds 2
        } while ((Get-Date) -lt $cleanupDeadline)

        if ($remainingReplicaRows -ne 0) {
            Write-Warning "Test passed, but cleanup did not reach the replica before timeout."
        }
        else {
            Write-Host "Recovery-test data cleaned from source and replica."
        }
    }
}

if (-not $testPassed) {
    exit 1
}
