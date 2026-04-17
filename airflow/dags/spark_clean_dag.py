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
    bash_command="docker run --rm --network=host -v /home/jurii/Projects/flight_analytic_pipeline/spark/jobs:/opt/spark-jobs flight_analytic_pipeline_spark-master:latest spark-submit --master spark://172.19.0.4:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/clean_flights.py",
)