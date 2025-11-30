from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Spark session
spark = SparkSession.builder \
    .appName("SyslogStreaming") \
    .getOrCreate()

# Kafka topic
topic = "syslog_raw"

# Schema for parsed JSON
schema = StructType([
    StructField("message", StringType(), True),
    StructField("hostname", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("facility", StringType(), True)
])

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Kafka messages → JSON string
json_df = df.selectExpr("CAST(value AS STRING) as json_string")

# Parse JSON
parsed = json_df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

# Simple transformation: filter only ERROR logs
filtered = parsed.filter(col("severity") == "error")

# Write to HDFS as Parquet
output = filtered.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/syslog/") \
    .option("path", "hdfs://namenode:8020/data/syslog/") \
    .outputMode("append") \
    .start()

output.awaitTermination()
