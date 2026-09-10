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

do {
    $metricLine = curl.exe -s http://localhost:8000/metrics |
        Select-String -Pattern "^cdc_orders_row_count_difference "

    if ($LASTEXITCODE -ne 0) {
        throw "Could not read CDC consistency metric."
    }

    if ($null -eq $metricLine) {
        throw "CDC consistency metric was not found."
    }

    $difference = [double](($metricLine.Line -split "\s+")[1])
    Write-Host "Current source/replica row difference: $difference"

    if ([double]$difference -eq 0) {
        break
    }

    Start-Sleep -Seconds 2
} while ((Get-Date) -lt $deadline)

if ([double]$difference -ne 0) {
    throw "Replica did not catch up within 120 seconds."
}

Write-Host "CDC load test passed. Replica caught up with no row-count difference."
