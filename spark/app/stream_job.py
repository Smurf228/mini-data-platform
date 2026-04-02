import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniodata123")
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://datalake/delta/customers_cdc")
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "s3a://datalake/checkpoints/customers_cdc",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_PATTERN = os.getenv("KAFKA_TOPIC_PATTERN", r"pg\.demo\.customers")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

spark = (
    SparkSession.builder.appName("KafkaSparkStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    # Pattern subscription avoids startup failure if CDC topic appears later.
    .option("subscribePattern", KAFKA_TOPIC_PATTERN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING) as json")

schema = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField(
                        "after",
                        StructType(
                            [
                                StructField("id", IntegerType()),
                                StructField("customer_id", IntegerType()),
                                StructField("email", StringType()),
                                StructField("name", StringType()),
                                StructField("full_name", StringType()),
                            ]
                        ),
                    )
                ]
            ),
        )
    ]
)

parsed = df_string.select(from_json(col("json"), schema).alias("data"))

customers_raw = parsed.select("data.payload.after.*")

customers = customers_raw.select(
    coalesce(col("customer_id"), col("id")).alias("customer_id"),
    coalesce(col("name"), col("full_name")).alias("name"),
    col("email"),
).where(col("customer_id").isNotNull())

customers_with_metadata = customers.withColumn("ingested_at", current_timestamp())

query = (
    customers_with_metadata.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(DELTA_PATH)
)

query.awaitTermination()
