from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import os
import tempfile

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

BASE_URL = "https://www.eurocontrol.int/performance/data/download/OPDI/v002/flight_list"
YEAR = 2024

with DAG(
    dag_id="flight_opdi_ingestion_v2",  # ✅ renamed
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["flight", "opdi", "minio"],
) as dag:

    # -------------------------
    # TASK 1 — GENERATE URLS
    # -------------------------
    @task()
    def generate_urls():
        urls = [
            f"{BASE_URL}/flight_list_{YEAR}{month:02d}.parquet"
            for month in range(1, 13)
        ]
        logging.info(f"Generated {len(urls)} URLs")
        return urls

    # -------------------------
    # TASK 2 — DOWNLOAD
    # -------------------------
    @task()
    def download(url: str) -> str:
        import requests

        tmp_dir = tempfile.mkdtemp()
        filename = os.path.join(tmp_dir, url.split("/")[-1])

        logging.info(f"Downloading {url}")

        r = requests.get(url, stream=True)
        logging.info(f"STATUS: {r.status_code}")

        if r.status_code != 200:
            raise ValueError(f"Download failed: {url}")

        with open(filename, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

        logging.info(f"Saved to {filename}")
        logging.info(f"SIZE: {os.path.getsize(filename)} bytes")

        return filename

    # -------------------------
    # TASK 3 — PARTITION + UPLOAD
    # -------------------------
    @task()
    def partition_and_upload(file_path: str) -> str:
        import pandas as pd
        import boto3

        logging.info(f"Processing {file_path}")

        df = pd.read_parquet(file_path)

        if df.empty:
            raise ValueError("Empty dataset")

        logging.info(f"Rows: {len(df)}")
        logging.info(f"Columns: {list(df.columns)}")

        # -------------------------
        # TIMESTAMP (ROBUST)
        # -------------------------
        if "first_seen" not in df.columns:
            raise ValueError(f"'first_seen' not found. Columns: {df.columns}")

        df["first_seen"] = pd.to_datetime(df["first_seen"], errors="coerce")

        null_ratio = df["first_seen"].isnull().mean()
        logging.info(f"Null timestamp ratio: {null_ratio}")

        if null_ratio > 0.2:
            logging.warning("Too many null timestamps, skipping file")
            return f"{file_path} skipped"

        # -------------------------
        # TYPE CLEANING
        # -------------------------
        for col in [
            "icao24",
            "callsign",
            "estdepartureairport",
            "estarrivalairport",
        ]:
            if col in df.columns:
                df[col] = df[col].astype("string")

        # -------------------------
        # PARTITIONING
        # -------------------------
        df["year"] = df["first_seen"].dt.year
        df["month"] = df["first_seen"].dt.month
        df["day"] = df["first_seen"].dt.day

        # -------------------------
        # MINIO (boto3)
        # -------------------------
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        bucket = "flight-data"
        prefix = "raw/flights"

        written = 0

        for (y, m, d), group in df.groupby(["year", "month", "day"]):

            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                group.to_parquet(tmp.name, index=False)

                base_name = os.path.basename(file_path).replace(".parquet", "")
                object_key = (
                    f"{prefix}/year={y}/month={m}/day={d}/"
                    f"{base_name}_{y}_{m}_{d}.parquet"
                )

                logging.info(f"Uploading → {object_key}")

                try:
                    s3.upload_file(tmp.name, bucket, object_key)
                    logging.info("UPLOAD SUCCESS")
                except Exception as e:
                    logging.error(f"UPLOAD FAILED: {e}")
                    raise

                written += 1

        logging.info(f"Written {written} partitions")

        # cleanup
        os.remove(file_path)

        return f"{file_path} → {written} partitions"

    # -------------------------
    # FLOW
    # -------------------------
    urls = generate_urls()
    files = download.expand(url=urls)
    partition_and_upload.expand(file_path=files)