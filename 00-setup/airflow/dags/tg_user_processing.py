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

user_processing_task_groups = []


def _extract_user(ti: TaskInstance, page_no=1):
    conn = BaseHook.get_connection("dummy_api")
    res = requests.get(
        conn.host + f"users?limit={PAGE_SIZE}&skip={(page_no - 1) * PAGE_SIZE}"
    )
    res = res.json()
    print(res)
    users = res["users"]
    ti.xcom_push(key="users", value=users)


def _process_user(ti, page_no=1):
    users = ti.xcom_pull(
        task_ids=f"user_processing_tg_{page_no}.extract_user", key="users"
    )
    processed_users = [
        {
            "id": user["id"],
            "firstname": user["firstName"],
            "lastname": user["lastName"],
            "country": user["address"]["country"],
            "username": user["username"],
            "password": user["password"],
            "email": user["email"],
            "age": user["age"],
            "gender": user["gender"],
        }
        for user in users
    ]

    processed_users = json_normalize(processed_users)

    processed_users.to_csv(
        f"/tmp/processed_user_{page_no}.csv", index=False, header=False
    )


with DAG(
    "tg_user_processing",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="dummy_api",
        endpoint="users?limit=10",
        timeout=60,
        poke_interval=20,
    )

    for page_no in pages:
        with TaskGroup(group_id=f"user_processing_tg_{page_no}") as user_processing_tg:
            extract_user = PythonOperator(
                task_id="extract_user",
                python_callable=_extract_user,
                op_kwargs={"page_no": page_no},
            )

            process_user = PythonOperator(
                task_id="process_user",
                python_callable=_process_user,
                op_kwargs={"page_no": page_no},
                outlets=[Dataset(uri=f"/tmp/processed_user_{page_no}.csv")],
            )

            extract_user >> process_user

            user_processing_task_groups.append(user_processing_tg)

    is_api_available >> user_processing_task_groups
