from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_clean_flights",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "transform"],
) as dag:

    spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/clean_flights.py
        """,
    )