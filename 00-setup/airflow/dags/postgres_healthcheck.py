from datetime import datetime, timedelta
import requests
import json

from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from include.on_failure_callback import trigger_alert_on_failure

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# TARGET_TABLE = "fact_view"
# FRESHNESS_THRESHOLD_HOURS = 24


@dag(
    dag_id="Postgres_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    on_failure_callback=trigger_alert_on_failure,
    catchup=False,
    tags=["postgres", "healthcheck"],
)
def postgres_healthcheck():

    @task(task_id="check_postgres_connection")
    def check_postgres_connection():
        pg_hook = PostgresHook(postgres_conn_id="postgres_spark_db")

        tables_df = pg_hook.get_pandas_df(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        table_list = tables_df["table_name"].tolist()

        db_info = pg_hook.get_first("SELECT version();")
        active_queries = pg_hook.get_pandas_df(
            "SELECT pid, state, query FROM pg_stat_activity WHERE state = 'active'"
        )

        print(f"Connected to: {db_info[0]}")
        print(f"Tables found: {table_list}")
        print(f"Active Queries Log:\n{active_queries.to_string()}")

        return table_list

        # pg_hook.get_conn()
        # print("PostgreSQL connection is healthy.")

    _table_list = check_postgres_connection()


postgres_healthcheck()
