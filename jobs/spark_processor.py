from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import uuid




KAFKA_BROKERS="kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financials_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALY_TOPIC = "transaction_anomalies"
CHECKPOINT_DIR = "/mnt/checkpoints"
CHECKPOINT_DIR_AGG = f"{CHECKPOINT_DIR}/aggregates_{uuid.uuid4()}"
STATES_DIR = "/mnt/spark-state"



spark = (SparkSession
         .builder
         .appName("FinancialTransactionProcessor")
         .config('spark.jars.package','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
         .config('spark.sql.shuffle.partition', 20)
         ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


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

kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                ).load()

transaction_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), transaction_schema).alias("data")) \
    .select("data.*")

transaction_df = transaction_df.withColumn('transactionTimestamp',
                                           (col("transactionTime") / 1000).cast("timestamp"))

aggregated_df = transaction_df.groupBy("merchantId") \
                 .agg(
                    sum("amount").alias("totalAmount"),
                    count("*").alias("transactionCount")
                 )


aggregated_query = (aggregated_df.withColumn("key",col("merchantId").cast("string")) \
                    .withColumn("value", to_json(struct(
                    col("merchantId"),
                    col("totalAmount"),
                    col("transactionCount")
                    )))
                    .selectExpr("key", "value")
                    .writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                    .outputMode("update")
                    .option("topic", AGGREGATES_TOPIC)
                    .option("checkpointLocation", CHECKPOINT_DIR_AGG)
                    )


aggregated_query.start().awaitTermination()



