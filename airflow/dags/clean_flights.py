from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, upper

spark = SparkSession.builder \
    .appName("flight-cleaning") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# -------------------------
# READ BRONZE
# -------------------------
df = spark.read.parquet("s3a://flight-data/bronze/flights/")

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
# ADD PARTITIONS
# -------------------------
df_clean = df_clean \
    .withColumn("year", year("first_seen_ts")) \
    .withColumn("month", month("first_seen_ts")) \
    .withColumn("day", dayofmonth("first_seen_ts"))

# -------------------------
# WRITE SILVER
# -------------------------
df_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://flight-data/silver/flights/")