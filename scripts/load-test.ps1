param(
    [int]$TotalRows = 1000,
    [int]$BatchSize = 100,
    [int]$DelayMilliseconds = 500
)

$ErrorActionPreference = "Stop"

if ($TotalRows -lt 1) {
    throw "TotalRows must be at least 1."
}

if ($BatchSize -lt 1) {
    throw "BatchSize must be at least 1."
}

$runId = Get-Date -Format "yyyyMMddHHmmss"
$insertedRows = 0
$batchNumber = 0

Write-Host "Starting CDC load test: $TotalRows rows, batch size $BatchSize"

while ($insertedRows -lt $TotalRows) {
    $currentBatchSize = [Math]::Min($BatchSize, $TotalRows - $insertedRows)
    $batchNumber++

    $sql = @"
INSERT INTO public.orders (customer_name, amount, status)
SELECT
  'Load-$runId-$batchNumber-' || generated_id,
  ROUND((100 + random() * 9900)::numeric, 2),
  'Pending'
FROM generate_series(1, $currentBatchSize) AS generated_id;
"@

    docker compose exec -T source-postgres psql `
        -v ON_ERROR_STOP=1 `
        -U postgres `
        -d source_db `
        -c $sql

    if ($LASTEXITCODE -ne 0) {
        throw "Load-test insert failed in batch $batchNumber."
    }

    $insertedRows += $currentBatchSize
    Write-Host "Inserted $insertedRows / $TotalRows rows"
    Start-Sleep -Milliseconds $DelayMilliseconds
}

Write-Host "Writes finished. Waiting for the replica to catch up..."

$deadline = (Get-Date).AddSeconds(120)
$sourceCount = $insertedRows
$replicaCount = 0

do {
    $replicaResult = docker compose exec -T replica-postgres psql `
        -X `
        -qAt `
        -v ON_ERROR_STOP=1 `
        -U postgres `
        -d replica_db `
        -c "SELECT COUNT(*) FROM public.orders WHERE customer_name LIKE 'Load-$runId-%';"

    if ($LASTEXITCODE -ne 0) {
        throw "Could not count this load-test run in the replica."
    }

    $replicaCount = [int](
        $replicaResult |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Select-Object -Last 1
    )

    Write-Host "Current run replicated: $replicaCount / $sourceCount rows"

    if ($replicaCount -eq $sourceCount) {
        break
    }

    Start-Sleep -Seconds 2
} while ((Get-Date) -lt $deadline)

if ($replicaCount -ne $sourceCount) {
    throw "Replica did not receive every row from this load-test run within 120 seconds."
}

Write-Host "CDC load test passed. Replica received all $sourceCount rows from this run."
