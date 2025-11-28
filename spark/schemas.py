from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Schema for JSON produced by syslog-ng
syslog_schema = StructType([
    StructField("date", StringType(), True),
    StructField("host", StringType(), True),
    StructField("program", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("message", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("level", IntegerType(), True)   # numeric syslog severity
])
