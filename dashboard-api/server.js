const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 8091;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "kafka:9092").split(",");
const DEFAULT_TOPIC = process.env.KAFKA_TOPIC || "pg.demo.customers";
const DEFAULT_GROUP = process.env.KAFKA_GROUP || "cdc-consumer-live";
const SPARK_MASTER_API = process.env.SPARK_MASTER_API || "http://spark-master:8080/json";

const dbPool = new Pool({
  host: process.env.DB_HOST || "postgres",
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME || "appdb",
  user: process.env.DB_USER || "app",
  password: process.env.DB_PASSWORD || "root"
});

app.use(cors());

const serviceChecks = [
  { id: "kafka-ui", name: "Kafka UI", url: "http://kafka-ui:8080" },
  { id: "spark-master", name: "Spark Master UI", url: "http://spark-master:8080" },
  { id: "spark-worker", name: "Spark Worker UI", url: "http://spark-worker:8081" },
  { id: "minio", name: "MinIO UI", url: "http://minio:9001" },
  { id: "connect", name: "Kafka Connect", url: "http://connect:8083/connectors" }
];

async function checkUrl(url, timeoutMs = 2500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const started = Date.now();
  try {
    const response = await fetch(url, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal
    });

    clearTimeout(timer);
    return {
      up: response.status < 500,
      statusCode: response.status,
      latencyMs: Date.now() - started
    };
  } catch (error) {
    clearTimeout(timer);
    return {
      up: false,
      statusCode: null,
      latencyMs: Date.now() - started,
      error: error.name || "Error"
    };
  }
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "dashboard-api" });
});

app.get("/api/status", async (_req, res) => {
  const results = await Promise.all(
    serviceChecks.map(async (service) => {
      const result = await checkUrl(service.url);
      return {
        ...service,
        ...result
      };
    })
  );

  res.json({
    generatedAt: new Date().toISOString(),
    services: results
  });
});

app.get("/api/kafka-lag", async (req, res) => {
  const topic = req.query.topic || DEFAULT_TOPIC;
  const groupId = req.query.groupId || DEFAULT_GROUP;

  const kafka = new Kafka({
    clientId: "dashboard-api",
    brokers: KAFKA_BROKERS
  });

  const admin = kafka.admin();

  try {
    await admin.connect();

    const topicOffsets = await admin.fetchTopicOffsets(topic);
    const groupOffsets = await admin.fetchOffsets({ groupId, topic });

    const groupByPartition = new Map(
      groupOffsets.map((item) => [item.partition, Number(item.offset)])
    );

    const partitions = topicOffsets.map((item) => {
      const latest = Number(item.offset);
      const committedRaw = groupByPartition.get(item.partition);
      const committed = committedRaw == null || committedRaw < 0 ? 0 : committedRaw;
      const lag = Math.max(0, latest - committed);

      return {
        partition: item.partition,
        latestOffset: latest,
        committedOffset: committed,
        lag
      };
    });

    const totalLag = partitions.reduce((acc, p) => acc + p.lag, 0);

    res.json({
      generatedAt: new Date().toISOString(),
      topic,
      groupId,
      totalLag,
      partitions
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to fetch Kafka lag",
      details: error.message,
      topic,
      groupId
    });
  } finally {
    await admin.disconnect();
  }
});

app.get("/api/spark-jobs", async (_req, res) => {
  try {
    const response = await fetch(SPARK_MASTER_API, { method: "GET" });
    if (!response.ok) {
      throw new Error(`Spark API returned HTTP ${response.status}`);
    }

    const payload = await response.json();
    const activeApps = payload.activeapps || [];
    const completedApps = payload.completedapps || [];
    const failedStates = new Set(["FAILED", "KILLED", "ERROR"]);

    const failedCount = completedApps.filter((app) =>
      failedStates.has(String(app.state || "").toUpperCase())
    ).length;

    res.json({
      generatedAt: new Date().toISOString(),
      sparkStatus: payload.status || "UNKNOWN",
      active: activeApps.length,
      completed: completedApps.length,
      failed: failedCount,
      runningApps: activeApps.map((app) => ({
        id: app.id,
        name: app.name,
        state: app.state,
        durationMs: app.duration
      }))
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to fetch Spark jobs status",
      details: error.message
    });
  }
});

app.post("/api/generate-data", async (_req, res) => {
  const now = Date.now();
  const customerId = 700000 + (now % 100000);
  const name = `UI Demo ${new Date(now).toISOString().slice(11, 19)}`;
  const email = `ui-${now}@example.com`;

  const query = `
    INSERT INTO demo.customers (customer_id, name, email)
    VALUES ($1, $2, $3)
    ON CONFLICT (customer_id) DO UPDATE
    SET name = EXCLUDED.name,
        email = EXCLUDED.email
    RETURNING customer_id, name, email;
  `;

  try {
    const result = await dbPool.query(query, [customerId, name, email]);
    res.json({
      generatedAt: new Date().toISOString(),
      inserted: result.rows[0]
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to insert generated data",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`dashboard-api listening on port ${PORT}`);
});
