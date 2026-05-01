from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, upper
import sys
import logging

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder \
    .appName("flight-cleaning") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.19.0.4:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

logging.info("Reading bronze data...")

df = spark.read.parquet("s3a://flight-data/bronze/flights/")

logging.info(f"Loaded {df.count()} rows")

# -------------------------
# CLEANING
# -------------------------
df_clean = df \
    .withColumn("first_seen_ts", to_timestamp("first_seen")) \
    .withColumn("last_seen_ts", to_timestamp("last_seen")) \
    .filter(col("first_seen_ts").isNotNull()) \
    .filter(col("last_seen_ts").isNotNull()) \
    .withColumn("icao24", col("icao24").cast("string")) \
    .withColumn("flt_id", col("flt_id").cast("string")) \
    .withColumn("icao_operator", col("icao_operator").cast("string")) \
    .withColumn("ADEP", upper(col("adep"))) \
    .withColumn("ADES", upper(col("ades")))

# -------------------------
# PARTITIONS
# -------------------------
df_clean = df_clean \
    .withColumn("year", year("first_seen_ts")) \
    .withColumn("month", month("first_seen_ts")) \
    .withColumn("day", dayofmonth("first_seen_ts"))

logging.info("Writing silver layer...")

df_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://flight-data/silver/flights/")

logging.info("Spark job completed successfully")