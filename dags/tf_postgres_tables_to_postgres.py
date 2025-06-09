from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


default_args  = {
    'owner': 'Wenderson Fagundes',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='tf-postgres-tables-to-postgre',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 29),
    catchup=False,
    tags=['postgres', 'postgres-operator', 'copy']
)
def postgres_tables_to_postgres():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
   
    create_table = PostgresOperator(
        task_id="create_dummy_table",
        postgres_conn_id="postgres_destino",
        sql="""
            CREATE TABLE IF NOT EXISTS public.drivers_batched (
            license_plate      TEXT,
            phone_number       TEXT,
            last_login         TIMESTAMP,
            email              TEXT,
            average_rating     NUMERIC,
            name               TEXT,
            total_earnings     NUMERIC,
            status             TEXT,
            registration_date  DATE,
            driver_id          TEXT PRIMARY KEY,
            total_deliveries   INTEGER,
            vehicle_type       TEXT
        );
        """
    )


    @task()
    def extract_data():
        try:
            hook = PostgresHook(postgres_conn_id='kates-integration-systems')
            record = hook.get_records("""SELECT * FROM public.drivers_batched;""")
            return record
        except Exception as e:
            print(f"Error extracting data: {str(e)}")
            raise

    
    @task
    def build_insert_query(records: list):
        if not records:
            return "SELECT 1;"
        
        inserts= []
        for row in records:
            values = ", ".join(
                [f"'{str(col).replace('\'', '\'\'')}'" if col is not None else "NULL" for col in row]
            )
            inserts.append(f"INSERT INTO public.drivers_batched VALUES ({values}) ON CONFLICT (driver_id) DO NOTHING;")

        return "\n".join(inserts)
    
    data_extract = extract_data()
    insert_query = build_insert_query(data_extract)

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_destino',
        sql=insert_query,
        autocommit=True
    )
    


    start >> create_table >> data_extract >> insert_query >> load_data >> end


dag = postgres_tables_to_postgres()

