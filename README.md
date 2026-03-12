# Mini Data Platform (Docker)

This project implements a **mini data platform** using containerized technologies to simulate a real-time data pipeline.

The system captures operational database changes and processes them through a streaming architecture.

## Architecture

```
UI / Backend
      ↓
Operational Database (PostgreSQL)
      ↓
Debezium CDC
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Data Lake (MinIO / Delta / Parquet)
```

## Technologies

* Docker & Docker Compose
* PostgreSQL
* Debezium (Change Data Capture)
* Apache Kafka
* Apache Spark Structured Streaming
* MinIO (S3-compatible Data Lake)
* Python

## Project Goals

The goal of this project is to simulate a **modern data engineering pipeline**, where:

1. Business events are generated and stored in PostgreSQL.
2. Debezium captures database changes (CDC).
3. Events are streamed through Kafka.
4. Spark processes the stream in real time.
5. Processed data is stored in a Data Lake.

## Project Structure

```
mini-data-platform
│
├── docker-compose.yml
├── README.md
├── .gitignore
│
├── app
│   └── simulate_business.py
│
└── data
    ├── customers.csv
    ├── orders.csv
    └── products.csv
```

## Planned Pipeline

```
Python Simulator
        ↓
PostgreSQL
        ↓
Debezium CDC
        ↓
Kafka Topics
        ↓
Spark Streaming
        ↓
MinIO Data Lake
```
