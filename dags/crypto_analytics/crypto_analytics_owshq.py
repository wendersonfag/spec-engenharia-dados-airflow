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

