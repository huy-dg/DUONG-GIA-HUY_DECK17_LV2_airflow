from datetime import datetime

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from pandas import json_normalize

pages = [1, 2, 3]

PAGE_SIZE = 10

user_storing_task_groups = []

csv_ds = [Dataset(uri=f"/tmp/processed_user_{page_no}.csv") for page_no in pages]


def _store_user(page_no=1):
    hook = PostgresHook(postgres_conn_id="postgres", database="airflow")

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename=f"/tmp/processed_user_{page_no}.csv",
    )


with DAG(
    "tg_store_users",
    schedule=csv_ds,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        database="airflow",
        sql="""
        DROP TABLE IF EXISTS users;
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT NOT NULL,
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            gender TEXT NOT NULL
        );
        """,
    )

    for page_no in pages:
        with TaskGroup(group_id=f"user_storing_tg_{page_no}") as user_storing_tg:
            store_user = PythonOperator(
                task_id="store_user",
                python_callable=_store_user,
                op_kwargs={"page_no": page_no},
            )

            user_storing_task_groups.append(user_storing_tg)

    create_table >> user_storing_task_groups
