from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="dbt_flights_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "silver"],
) as dag:

    run_staging = BashOperator(
        task_id="run_stg_flights",
        bash_command="""
        docker exec dbt dbt run --select stg_flights
        """
    )

    run_silver = BashOperator(
        task_id="run_flights_clean",
        bash_command="""
        docker exec dbt dbt run --select flights_clean
        """
    )

    run_staging >> run_silver