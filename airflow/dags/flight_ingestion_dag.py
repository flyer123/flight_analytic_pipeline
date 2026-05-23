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
YEAR = 2025

with DAG(
    dag_id="flight_opdi_ingestion_v3_schema_safe",
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

        filename = os.path.join(
            tmp_dir,
            url.split("/")[-1]
        )

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
        import pyarrow as pa
        import pyarrow.parquet as pq
        import traceback

        logging.info(f"Processing file: {file_path}")

        try:

            # -------------------------
            # READ FILE
            # -------------------------
            df = pd.read_parquet(file_path)

            logging.info("PARQUET READ SUCCESS")

            if df.empty:
                raise ValueError("Empty dataset")

            logging.info(f"Rows: {len(df)}")
            logging.info(f"Columns: {list(df.columns)}")

            # -------------------------
            # NORMALIZE TIMESTAMPS
            # -------------------------
            timestamp_columns = [
                "first_seen",
                "last_seen",
                "dof",
            ]

            for col in timestamp_columns:

                if col in df.columns:

                    logging.info(f"Normalizing timestamp: {col}")

                    df[col] = pd.to_datetime(
                        df[col],
                        errors="coerce",
                    )

                    # IMPORTANT
                    df[col] = df[col].astype("datetime64[ns]")

                    logging.info(
                        f"{col} dtype => {df[col].dtype}"
                    )

            # -------------------------
            # VALIDATION
            # -------------------------
            if "first_seen" not in df.columns:
                raise ValueError(
                    "first_seen column missing"
                )

            null_ratio = df["first_seen"].isnull().mean()

            logging.info(
                f"first_seen null ratio: {null_ratio}"
            )

            if null_ratio > 0.2:
                raise ValueError(
                    "Too many invalid timestamps"
                )

            # -------------------------
            # STRING CLEANING
            # -------------------------
            string_columns = [
                "icao24",
                "callsign",
                "flt_id",
                "icao_operator",
                "adep",
                "ades",
                "estdepartureairport",
                "estarrivalairport",
            ]

            for col in string_columns:

                if col in df.columns:

                    df[col] = (
                        df[col]
                        .astype("string")
                    )
            
            #--------------------------
            # ENSURE ID COLUMN IS NORMALIZED
            #--------------------------
            if "id" in df.columns:
                df["id"] = (
                    df["id"]
                    .astype("string")
                )

            # -------------------------
            # PARTITIONS
            # -------------------------
            df["year"] = df["first_seen"].dt.year
            df["month"] = df["first_seen"].dt.month
            df["day"] = df["first_seen"].dt.day

            df = df.dropna(
                subset=["year", "month", "day"]
            )

            logging.info(
                f"DataFrame after cleanup: {len(df)} rows"
            )

            # -------------------------
            # MINIO
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

            # -------------------------
            # WRITE PARTITIONS
            # -------------------------
            for (y, m, d), group in df.groupby(
                ["year", "month", "day"]
            ):

                logging.info(
                    f"Partition => "
                    f"year={y}, month={m}, day={d}, "
                    f"rows={len(group)}"
                )

                try:

                    logging.info(
                        f"Dtypes:\n{group.dtypes}"
                    )

                    tmp = tempfile.NamedTemporaryFile(
                        suffix=".parquet",
                        delete=False,
                    )

                    tmp.close()

                    # IMPORTANT:
                    # let Arrow infer schema naturally
                    # AFTER normalization
                    table = pa.Table.from_pandas(
                        group,
                        preserve_index=False,
                    )

                    logging.info(
                        f"Arrow schema:\n{table.schema}"
                    )

                    pq.write_table(
                        table,
                        tmp.name,
                        compression="snappy",
                        coerce_timestamps="ms",
                        allow_truncated_timestamps=True,
                    )

                    base_name = os.path.basename(
                        file_path
                    ).replace(
                        ".parquet",
                        ""
                    )

                    object_key = (
                        f"{prefix}/"
                        f"year={int(y)}/"
                        f"month={int(m)}/"
                        f"day={int(d)}/"
                        f"{base_name}_{y}_{m}_{d}.parquet"
                    )

                    logging.info(
                        f"Uploading => {object_key}"
                    )

                    s3.upload_file(
                        tmp.name,
                        bucket,
                        object_key,
                    )

                    logging.info("UPLOAD SUCCESS")

                    written += 1

                    os.remove(tmp.name)

                except Exception as partition_error:

                    logging.error(
                        f"FAILED PARTITION "
                        f"{y}-{m}-{d}"
                    )

                    logging.error(
                        traceback.format_exc()
                    )

                    raise partition_error

            logging.info(
                f"Written partitions: {written}"
            )

            os.remove(file_path)

            return (
                f"{file_path} -> "
                f"{written} partitions"
            )

        except Exception as e:

            logging.error(
                "FULL TASK FAILURE"
            )

            logging.error(
                traceback.format_exc()
            )

            raise e

    # -------------------------
    # FLOW
    # -------------------------
    urls = generate_urls()

    files = download.expand(
        url=urls
    )

    partition_and_upload.expand(
        file_path=files
    )