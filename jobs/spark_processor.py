from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
SOURCE_TOPIC = "financials_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALY_TOPIC = "transaction_anomalies"
CHECKPOINT_DIR = "/mnt/checkpoints"
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