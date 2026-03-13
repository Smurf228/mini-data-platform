from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pg.demo.customers")
    .option("startingOffsets", "earliest")
    .load()
)

df_string = df.selectExpr("CAST(value AS STRING) as json")

schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("email", StringType()),
            StructField("full_name", StringType())
        ]))
    ]))
])

parsed = df_string.select(
    from_json(col("json"), schema).alias("data")
)

customers = parsed.select("data.payload.after.*")

query = (
    customers.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()