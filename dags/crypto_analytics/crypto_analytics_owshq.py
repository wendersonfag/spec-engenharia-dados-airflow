# TODO define libraries and imports
from airflow import Dataset
from airflow. decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

# TODO define location using __init__.py
from crypto_analytics.tasks import(
    fetch_price_data,
    fetch_volume_data,
    clean_data,
    aggregate_metrics,
    generate_trends,
    check_volatility
)


# TODO define datasets
price_dataset = Dataset("crypto/price_data")
volume_dataset = Dataset("crypto/volume_data")  
aggregate_dataset = Dataset("crypto/aggregate_data")

# TODO DAG and task defaults
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="crypto_analytics_owshq",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 5),
    catchup=False,
    tags=["crypto", "micropipelines", "airflow2.4"]
)
def crypto_data_pipeline():
    crypto_list = ["BTC", "ETH", "ADA", "LUNA", "SOL"]

    # TODO Task Group for Data Processing
    with TaskGroup("data_fetching", tooltip="Fetch Price and Volume Data") as data_fetching:
        fetch_price_tasks = fetch_price_data.expand(crypto=crypto_list)
        fetch_volume_tasks = fetch_volume_data.expand(crypto=crypto_list)

    # TODO Task Group for Data Processing
    with TaskGroup("data_processing", tooltip="Clean and Aggregate Data") as data_processing:
        cleaned_data = clean_data(price_dataset, volume_dataset)
        aggregated_data = aggregate_metrics(cleaned_data)

    # TODO Task Group for Data Analysis
    with TaskGroup("data_analysis", tooltip="Analyze Trends and Check Volatility") as data_analysis:
        trends= generate_trends(aggregated_data)
        volatility_alert = check_volatility(trends)

    # TODO Define task dependencies using chain for linear structure
    chain(
        data_fetching,
        data_processing,
        data_analysis
    )

crypto_data_dag = crypto_data_pipeline()   
