from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
import re
import os

BUCKET = "flight-data"
PREFIX = "bronze/flights/"
LOCAL_TMP_DIR = "/tmp/minio_files"

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_CONN_ID = "minio_s3"

# ✅ DATE RANGE (controlled in DAG)
DATE_START = datetime(2024, 1, 1)
DATE_END   = datetime(2024, 1, 2)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="minio_to_snowflake_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["minio", "snowflake", "bronze"],
) as dag:

    @task
    def init_snowflake():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        sql = """
        USE SCHEMA BRONZE;
        CREATE FILE FORMAT IF NOT EXISTS parquet_format
        TYPE = PARQUET;

        CREATE TABLE IF NOT EXISTS flights_stage (
            raw VARIANT,
            year INT,
            month INT,
            day INT,
            source_file STRING,
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );

        CREATE TABLE IF NOT EXISTS flights (
            flight_id STRING,
            departure TIMESTAMP,
            arrival TIMESTAMP,
            icao24 STRING,
            icao_operator STRING,
            ADEP STRING,
            ADES STRING,
            year INT,
            month INT,
            day INT,
            load_ts TIMESTAMP
        );
        """

        hook.run(sql)

    @task
    def list_files():
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)

        keys = s3.list_keys(bucket_name=BUCKET, prefix=PREFIX)
        if not keys:
            return []

        pattern = re.compile(r"year=(\d+)/month=(\d+)/day=(\d+)")

        filtered = []

        for k in keys:
            # skip folders / system files
            if k.endswith("/") or k.split("/")[-1].startswith("_"):
                continue

            match = pattern.search(k)
            if not match:
                continue

            year, month, day = map(int, match.groups())
            file_date = datetime(year, month, day)

            # ✅ date filter
            if DATE_START <= file_date <= DATE_END:
                filtered.append(k)

        return filtered

    def get_unique_path(base_path):
        """Rename file if exists: file → file_1 → file_2"""
        if not os.path.exists(base_path):
            return base_path

        name, ext = os.path.splitext(base_path)
        counter = 1

        while True:
            new_path = f"{name}_{counter}{ext}"
            if not os.path.exists(new_path):
                return new_path
            counter += 1

    @task
    def download_files(keys: list):
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)

        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

        local_files = []

        for key in keys:
            obj = s3.get_key(key, bucket_name=BUCKET)

            safe_name = key.replace("/", "_")
            base_path = os.path.join(LOCAL_TMP_DIR, safe_name)

            # ✅ collision-safe path
            local_path = get_unique_path(base_path)

            with open(local_path, "wb") as f:
                f.write(obj.get()["Body"].read())

            local_files.append((key, local_path))

        return local_files

    @task
    def upload_and_copy(local_files: list):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        pattern = re.compile(r"year=(\d+)/month=(\d+)/day=(\d+)")

        for key, local_path in local_files:
            match = pattern.search(key)
            if not match:
                continue

            year, month, day = match.groups()

            put_sql = f"""
            PUT file://{local_path} @%flights_stage AUTO_COMPRESS=TRUE;
            """
            hook.run(put_sql)

            copy_sql = f"""
            COPY INTO flights_stage (raw, year, month, day, source_file)
            FROM (
                SELECT
                    $1,
                    {year},
                    {month},
                    {day},
                    '{key}'
                FROM @%flights_stage
            )
            FILE_FORMAT = (FORMAT_NAME = parquet_format)
            PATTERN = '.*{os.path.basename(local_path)}.*';
            """
            hook.run(copy_sql)

    @task
    def insert_into_final():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        sql = """
        INSERT INTO flights
        SELECT
            raw:flt_id::STRING,
            raw:first_seen_ts::TIMESTAMP,
            raw:last_seen_ts::TIMESTAMP,
            raw:icao24::STRING,
            raw:icao_operator::STRING,
            raw:ADEP::STRING,
            raw:ADES::STRING,
            year,
            month,
            day,
            load_ts
        FROM flights_stage;
        """

        hook.run(sql)

    init = init_snowflake()
    keys = list_files()
    files = download_files(keys)
    load = upload_and_copy(files)
    insert = insert_into_final()

    init >> keys >> files >> load >> insert