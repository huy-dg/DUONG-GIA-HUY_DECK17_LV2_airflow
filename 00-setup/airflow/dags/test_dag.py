# from datetime import datetime, timedelta
import requests
import random
import json

# from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
# from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook

# from airflow.exceptions import AirflowException, AirflowSkipException
# from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
# from confluent_kafka.admin import OffsetSpec
from include.on_failure_callback import trigger_alert_on_failure


@dag(
    dag_id="test_dag",
    schedule=None,
    on_failure_callback=trigger_alert_on_failure,
    catchup=False,
    tags=["test", "conn", "on_callback_failure"],
)
def test_dag():

    @task(task_id="conn_1")
    def test_1():
        conn = BaseHook.get_connection("dummy_api")
        res = requests.get(conn.host + "users?limit=1", timeout=10)
        res.raise_for_status()
        data = res.json()
        print(f"Data fetched: {data}")
        return random.choice(["users", "rubbish"])

    @task(task_id="conn_2")
    def test_2(test_1_result):
        conn = BaseHook.get_connection("dummy_api")
        res = requests.get(conn.host + f"{test_1_result}?limit=1", timeout=10)
        res.raise_for_status()
        data = res.json()["users"][0]
        keyword = data[list(data.keys())[random.randint(0, len(data) - 1)]]
        print(f"Data fetched: {keyword}")

        return str(keyword)

    test_1_result = test_1()
    test_2_result = test_2(test_1_result)

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_telegram_alert",
        trigger_dag_id="telegram_bot_alert",
        conf={
            "triggerer": "test_program",
            "message": test_2_result,
        },
        # trigger_rule="all_done",
        wait_for_completion=False,
        poke_interval=10,
    )

    trigger_alert


test_dag()
