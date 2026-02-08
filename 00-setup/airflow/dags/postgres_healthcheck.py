from datetime import datetime, timedelta
import requests
import json

from airflow.decorators import dag, task
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
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
        pg_hook = PostgresHook(postgres_conn_id="postgres", database="airflow")

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

    _table_list = check_postgres_connection()

    @task(task_id="inspect_schema")
    def inspect_schema(table_list):
        hook = PostgresHook(postgres_conn_id="postgres", database="airflow")
        # schema_report = []
        test_table = table_list[0]

        # Primary Key query
        pk_query = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{test_table}'::regclass AND i.indisprimary;
        """
        pk: tuple = hook.get_first(pk_query)

        # Columns names query
        cols = hook.get_pandas_df(
            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{test_table}'"
        )
        cols_dict = cols.to_dict(orient="records")
        cols_str = "\n".join(
            [f"{d['column_name']} --- {d['data_type']}" for d in cols_dict]
        )
        postgres_report = (
            f"Table: {test_table}\n"
            f"PK: {pk[0] if pk else 'None'}\n"
            f"Cols:\n**column_name --- data_type**\n{cols_str}"
        )

        return postgres_report

    _postgres_report = inspect_schema(_table_list)

    # Task 3: check data quality
    # check_fact_table_quality = SQLColumnCheckOperator(
    #     task_id="check_fact_table_quality",
    #     conn_id="postgres_spark_db",
    #     table="fact_view",
    #     column_mapping={
    #         "fact_key": {"null_check": {"equal_to": 0}},  # Không được có dòng nào null
    #         # "percentage": {
    #         #     "min": {"greater_than": 10},  # percentage > 10
    #         #     "max": {"less_than_or_equal_to": 100},  # percentage <= 100
    #         # },
    #     },
    # )

    # Task 4: check data freshness
    # check_data_freshness = SqlSensor(
    #     task_id="check_data_freshness",
    #     conn_id="postgres_spark_db",
    #     sql="SELECT COUNT(*) FROM fact_view WHERE created_at >= NOW() - INTERVAL '24 hours'",
    #     success_check=lambda res: res > 0,
    #     poke_interval=60,
    #     timeout=120,
    #     mode="reschedule",
    # )

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_telegram_alert",
        trigger_dag_id="telegram_bot_alert",
        conf={
            "triggerer": "postgres_healthcheck",
            "message": _postgres_report,
        },
        # trigger_rule="all_done",
        wait_for_completion=False,
        poke_interval=10,
    )

    trigger_alert


postgres_healthcheck()
