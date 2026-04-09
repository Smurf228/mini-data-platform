# Mini Data Platform (Docker)

Mini data platform for CDC streaming:

PostgreSQL -> Debezium -> Kafka -> Spark Structured Streaming

## Current Scope

Implemented:

1. CSV simulator loads business data into PostgreSQL.
2. Debezium captures changes from PostgreSQL schema `demo`.
3. Kafka receives JSON CDC events.
4. Python consumer prints CDC events.
5. Spark job reads JSON events from Kafka and prints transformed rows.
6. MinIO + Delta Lake sink for processed data.
7. Automated deploy script with health checks and connector bootstrap.

Not implemented yet:

1. Additional reliability hardening and CI/CD pipeline.

## Services and Ports

1. Kafka broker (host): `localhost:29092`
2. PostgreSQL (host): `localhost:55432`
3. Kafka Connect REST: `http://localhost:8083`
4. Kafka UI: `http://localhost:8080`
5. Spark Master UI: `http://localhost:8081`
6. Spark Worker UI: `http://localhost:8082`
7. MinIO API: `http://localhost:9000`
8. MinIO Console: `http://localhost:9001`
9. Platform Dashboard: `http://localhost:8090`

## Quick Start

### Automated Deploy

One command deployment:

```powershell
cd D:\mini-data-platform
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
& .\scripts\deploy.ps1
```

From-scratch deployment (clean volumes) + smoke test:

```powershell
cd D:\mini-data-platform
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
& .\scripts\deploy.ps1 -ResetData -RunSmokeTest
```

What this script does:

1. Starts all Docker services.
2. Waits for Kafka Connect and MinIO endpoints.
3. Creates or updates Debezium connector from `postgres-connector.json`.
4. Waits until connector task is `RUNNING`.
5. Loads CSV data to PostgreSQL.
6. Optional smoke test for CDC message flow.

Open unified dashboard:

```text
http://localhost:8090
```

### 1) Start containers

```powershell
cd D:\mini-data-platform
docker compose up -d
docker ps
```

`connect-init` auto-registers `pg-customers-connector` during startup, so manual connector creation is usually not needed.

### 2) Ensure Debezium connector exists

```powershell
Invoke-RestMethod -Method Get -Uri http://localhost:8083/connectors
```

If `pg-customers-connector` is still missing:

```powershell
$body = Get-Content postgres-connector.json -Raw
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors -ContentType "application/json" -Body $body
```

### 3) Load CSV data into PostgreSQL

```powershell
d:/mini-data-platform/.venv/Scripts/python.exe app/simulate_business.py
```

### 4) Start Python CDC consumer (new terminal)

```powershell
cd D:\mini-data-platform
d:/mini-data-platform/.venv/Scripts/python.exe app/consumer.py
```

Default behavior: reads only new events (`CDC_OFFSET_RESET=latest`) with consumer group `cdc-consumer-live`.

If you need full replay from topic start:

```powershell
$env:CDC_OFFSET_RESET="earliest"
$env:CDC_GROUP_ID="cdc-consumer-replay"
d:/mini-data-platform/.venv/Scripts/python.exe app/consumer.py
```

### 5) Start Spark stream (new terminal)

```powershell
cd D:\mini-data-platform
docker exec spark-master bash -lc "mkdir -p /tmp/.ivy && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-app/stream_job.py"
```

Default behavior: reads only new events (`startingOffsets=latest`).

### 6) Generate a test event

```powershell
docker exec postgres psql -U app -d appdb -c "INSERT INTO demo.customers (customer_id, name, email) VALUES (1001, 'Step Check', 'step-check@example.com');"
```

Expected result:

1. `app/consumer.py` prints `NEW EVENT` with inserted row.
2. Spark stream writes event to Delta table in MinIO (`s3a://datalake/delta/customers_cdc`).

## Task 5: MinIO + Delta Lake

### MinIO Credentials

1. User: `minio`
2. Password: `miniodata123`

### Verify data in MinIO Console

1. Open `http://localhost:9001`
2. Login with credentials above.
3. Open bucket `datalake`.
4. Check folder `delta/customers_cdc`.

### Read Delta table with Spark

Use helper script:

```powershell
docker exec spark-master bash -lc "mkdir -p /tmp/.ivy && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-app/read_delta.py"
```

This prints latest rows from `s3a://datalake/delta/customers_cdc`.

## Data Contracts

Current simulator table `demo.customers` columns:

1. `customer_id`
2. `name`
3. `email`

Spark parser is made tolerant to both payload variants:

1. Legacy: `id`, `full_name`, `email`
2. Current: `customer_id`, `name`, `email`

Delta sink schema:

1. `customer_id`
2. `name`
3. `email`
4. `ingested_at`

## Troubleshooting

### Consumer starts but prints nothing

1. Confirm connector task is `RUNNING`:

```powershell
Invoke-RestMethod -Method Get -Uri http://localhost:8083/connectors/pg-customers-connector/status | ConvertTo-Json -Depth 10
```

2. Insert one fresh row and watch consumer output.

### Kafka timeout in `kafka-console-consumer`

Short timeouts can show zero messages even on healthy setup. Use:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic pg.demo.customers --from-beginning --max-messages 50
```

### Password mismatch between PostgreSQL and connector

Project default password is `root` for user `app`.

If needed, reset inside container:

```powershell
docker exec postgres psql -U app -d appdb -c "ALTER USER app WITH PASSWORD 'root';"
```

Then re-apply connector config:

```powershell
$cfg = Invoke-RestMethod -Method Get -Uri http://localhost:8083/connectors/pg-customers-connector/config
$cfg.'database.password' = 'root'
$cfgJson = $cfg | ConvertTo-Json -Compress
Invoke-RestMethod -Method Put -Uri http://localhost:8083/connectors/pg-customers-connector/config -ContentType "application/json" -Body $cfgJson
```

### Spark says "Initial job has not accepted any resources"

Reason: all worker cores are already occupied by another streaming app.

Fix:

1. Open `http://localhost:8081`.
2. Kill old `RUNNING` app.
3. Start only one streaming job at a time.
