from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="dbt_gold_models",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "silver"],
) as dag:

    run_gold = BashOperator(
        task_id="dbt_gold_run",
        bash_command="""
        docker exec dbt dbt run --select gold
        """
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="""
        docker exec dbt dbt test --select gold
        """
    )

    run_gold >> test_gold