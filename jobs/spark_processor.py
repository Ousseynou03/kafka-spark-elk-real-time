from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

# --- Config Kafka & Checkpoints ---
KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financials_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
CHECKPOINT_DIR = "/checkpoints"
CHECKPOINT_DIR_AGG = f"{CHECKPOINT_DIR}/aggregates"
STATES_DIR = "/spark-state"

# --- Spark session ---
spark = (SparkSession
         .builder
         .appName("FinancialTransactionProcessor")
         .config('spark.jars.package','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
         .config('spark.sql.shuffle.partitions', 20)
         ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Schema des transactions ---
transaction_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transactionTime", LongType(), True),
    StructField("merchantId", StringType(), True),
    StructField("transactionType", StringType(), True),
    StructField("location", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("isinternational", StringType(), True),
    StructField("currency", StringType(), True),
])

# --- Lecture du flux Kafka ---
kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load())

transaction_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), transaction_schema).alias("data")) \
    .select("data.*")

transaction_df = transaction_df.withColumn(
    'transactionTimestamp',
    (col("transactionTime") / 1000).cast("timestamp")
)

# --- Agrégats par merchant ---
aggregated_df = transaction_df.groupBy("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )

# --- Écriture vers Kafka ---
aggregated_query = (aggregated_df
                    .withColumn("key", col("merchantId").cast("string"))
                    .withColumn("value", to_json(struct(
                        col("merchantId"),
                        col("totalAmount"),
                        col("transactionCount")
                    )))
                    .selectExpr("key", "value")
                    .writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                    .option("topic", AGGREGATES_TOPIC)
                    .outputMode("update")
                    .option("checkpointLocation", CHECKPOINT_DIR_AGG)
                    )

aggregated_query.start().awaitTermination()
