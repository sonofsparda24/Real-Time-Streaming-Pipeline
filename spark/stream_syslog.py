import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, count, expr, to_json, struct
)
from pyspark.sql.types import StringType
from schemas import syslog_schema


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SyslogStreamingPipeline")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def severity_to_label(level):
    mapping = {
        0: "EMERGENCY",
        1: "ALERT",
        2: "CRITICAL",
        3: "ERROR",
        4: "WARNING",
        5: "NOTICE",
        6: "INFO",
        7: "DEBUG"
    }
    return mapping.get(level, "UNKNOWN")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ===============================================================
    # 1. READ FROM KAFKA
    # ===============================================================
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "syslog_raw")
        .option("startingOffsets", "latest")
        .load()
    )

    # Kafka stores messages as binary — convert to string
    json_df = raw_df.withColumn("value", col("value").cast(StringType()))

    # ===============================================================
    # 2. PARSE JSON
    # ===============================================================
    parsed_df = json_df.withColumn(
        "data", from_json(col("value"), syslog_schema)
    ).select("data.*")

    # ===============================================================
    # 3. ENRICHMENT
    # ===============================================================

    # Add processing timestamp
    enriched_df = parsed_df.withColumn("processing_time", current_timestamp())

    # Convert numeric level → textual severity
    severity_expr = expr("""
        CASE 
            WHEN level = 0 THEN 'EMERGENCY'
            WHEN level = 1 THEN 'ALERT'
            WHEN level = 2 THEN 'CRITICAL'
            WHEN level = 3 THEN 'ERROR'
            WHEN level = 4 THEN 'WARNING'
            WHEN level = 5 THEN 'NOTICE'
            WHEN level = 6 THEN 'INFO'
            WHEN level = 7 THEN 'DEBUG'
            ELSE 'UNKNOWN'
        END
    """)
    enriched_df = enriched_df.withColumn("severity_label", severity_expr)

    # Watermark for late data
    enriched_df = enriched_df.withWatermark("processing_time", "2 minutes")

    # ===============================================================
    # 4. ANALYTICS
    # ===============================================================

    # -- 4.1 Severity count per minute
    severity_counts = (
        enriched_df
        .groupBy(
            window(col("processing_time"), "1 minute"),
            col("severity_label")
        )
        .agg(count("*").alias("count"))
    )

    # -- 4.2 Top hosts generating ERRORs (5-min sliding window)
    error_hosts = (
        enriched_df.filter(col("severity_label") == "ERROR")
        .groupBy(
            window(col("processing_time"), "5 minutes", "1 minute"),
            col("host")
        )
        .agg(count("*").alias("error_count"))
        .orderBy(col("error_count").desc())
    )

    # ===============================================================
    # 5. ANOMALY DETECTION
    #    More than 5 errors from the same host in 2 minutes
    # ===============================================================

    anomalies = (
        enriched_df.filter(col("severity_label") == "ERROR")
        .groupBy(
            window(col("processing_time"), "2 minutes"),
            col("host")
        )
        .agg(count("*").alias("error_count"))
        .filter(col("error_count") > 5)
    )

    # Convert anomaly rows → JSON for Kafka
    anomalies_to_kafka = anomalies.select(
        to_json(
            struct(
                col("host"),
                col("error_count"),
                col("window").start.alias("window_start"),
                col("window").end.alias("window_end")
            )
        ).alias("value")
    )

    # ===============================================================
    # 6. WRITE OUTPUTS
    # ===============================================================

    # -- console (for debugging)
    console_query = (
        enriched_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    # -- HDFS: parquet
    parquet_query = (
        enriched_df.writeStream
        .format("parquet")
        .option("path", "/hdfs/parquet/syslog")
        .option("checkpointLocation", "/hdfs/checkpoints/syslog")
        .outputMode("append")
        .start()
    )

    # -- Alerts → Kafka topic syslog_alerts
    kafka_alerts = (
        anomalies_to_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", "syslog_alerts")
        .option("checkpointLocation", "/hdfs/checkpoints/alerts")
        .outputMode("append")
        .start()
    )

    console_query.awaitTermination()
    parquet_query.awaitTermination()
    kafka_alerts.awaitTermination()


if __name__ == "__main__":
    main()
