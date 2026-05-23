from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="dbt_silver_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "silver"],
) as dag:

    run_deps = BashOperator(
        task_id="run_dependencies",
        bash_command="""
        docker exec dbt dbt deps
        """
    )

    run_tests = BashOperator(
        task_id="run_tests",
        bash_command="""
        docker exec dbt dbt test --select flights_clean
        """
    )

    run_deps >> run_tests