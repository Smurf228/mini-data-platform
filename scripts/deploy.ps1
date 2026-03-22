Param(
    [switch]$ResetData,
    [switch]$RunSmokeTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Wait-HttpOk {
    Param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSeconds = 120,
        [int]$IntervalSeconds = 2
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $null = Invoke-RestMethod -Method Get -Uri $Url -TimeoutSec 5
            return
        }
        catch {
            Start-Sleep -Seconds $IntervalSeconds
        }
    }

    throw "Timeout waiting for $Url"
}

function Wait-ConnectorRunning {
    Param(
        [string]$Name,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $status = Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/$Name/status"
            if ($status.connector.state -eq "RUNNING" -and $status.tasks[0].state -eq "RUNNING") {
                return
            }
        }
        catch {
            # ignore until timeout
        }

        Start-Sleep -Seconds 2
    }

    throw "Connector $Name did not reach RUNNING state in time"
}

Write-Host "==> Starting platform deployment..."

if ($ResetData) {
    Write-Host "==> Reset mode enabled: removing containers and volumes"
    docker compose down -v
}

docker compose up -d --remove-orphans

Write-Host "==> Waiting for service endpoints"
Wait-HttpOk -Url "http://localhost:8083/connectors"
Wait-HttpOk -Url "http://localhost:9001"

Write-Host "==> Ensuring Debezium connector"
$connectorName = "pg-customers-connector"
$connectors = Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors"
$connectorBody = Get-Content "postgres-connector.json" -Raw

if ($connectors -contains $connectorName) {
    $cfg = Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/$connectorName/config"
    $desiredCfg = (ConvertFrom-Json $connectorBody).config

    # Apply source-of-truth values from file while preserving runtime-managed properties.
    foreach ($p in $desiredCfg.PSObject.Properties) {
        $cfg.($p.Name) = [string]$p.Value
    }

    $cfgJson = $cfg | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/$connectorName/config" -ContentType "application/json" -Body $cfgJson | Out-Null
}
else {
    Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -Body $connectorBody | Out-Null
}

Wait-ConnectorRunning -Name $connectorName

Write-Host "==> Loading base CSV data into PostgreSQL"
$env:DB_PASSWORD = "root"
$env:DB_PORT = "55432"
$pythonExe = "d:/mini-data-platform/.venv/Scripts/python.exe"
& $pythonExe "app/simulate_business.py"

if ($RunSmokeTest) {
    Write-Host "==> Running smoke test insert"
    $stamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $id = 900000 + ($stamp % 100000)
    $email = "smoke-$stamp@example.com"
    $name = "Smoke $stamp"

    docker exec postgres psql -U app -d appdb -c "INSERT INTO demo.customers (customer_id, name, email) VALUES ($id, '$name', '$email');" | Out-Null

    Write-Host "==> Verifying topic receives messages"
    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic pg.demo.customers --from-beginning --max-messages 1 | Out-Null

    Write-Host "==> Smoke test completed"
}

Write-Host "==> Deployment finished"
Write-Host "Kafka UI:        http://localhost:8080"
Write-Host "Spark Master UI: http://localhost:8081"
Write-Host "Spark Worker UI: http://localhost:8082"
Write-Host "Kafka Connect:   http://localhost:8083"
Write-Host "MinIO Console:   http://localhost:9001"
