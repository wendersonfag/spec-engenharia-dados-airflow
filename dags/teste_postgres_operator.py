from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="test_postgres_ok",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id="create_table_test",
        postgres_conn_id="kates-integration-systems",
        sql="SELECT 1;"
    )
