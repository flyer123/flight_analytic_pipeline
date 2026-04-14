from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, upper
import logging

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder \
    .appName("flight-cleaning") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

logging.info("Reading bronze data...")

df = spark.read.parquet("s3a://flight-data/bronze/flights/")

logging.info(f"Loaded {df.count()} rows")

# -------------------------
# CLEANING
# -------------------------
df_clean = df \
    .withColumn("first_seen_ts", to_timestamp("first_seen")) \
    .filter(col("first_seen_ts").isNotNull()) \
    .withColumn("icao24", col("icao24").cast("string")) \
    .withColumn("callsign", col("callsign").cast("string")) \
    .withColumn("estdepartureairport", upper(col("estdepartureairport"))) \
    .withColumn("estarrivalairport", upper(col("estarrivalairport")))

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