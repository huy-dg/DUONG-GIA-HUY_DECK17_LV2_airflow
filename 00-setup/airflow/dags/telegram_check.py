from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
import requests


@dag(
    dag_id="telegram_bot_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["alert", "test"],
)
def telegram_bot_test():
    @task(
        task_id="test_telegram_connection",
    )
    def test_telegram_connection():
        conn = BaseHook.get_connection("telegram_bot")
        payload = conn.get_extra_dejson()
        payload.update(
            {
                "text": f"Daily ping from Airflow:\n[{datetime.now()}]-[Telegram Bot Test DAG]: Online!"
            }
        )
        response = requests.post(
            url=conn.host + "bot" + conn.password + "/sendMessage",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        print("Message sent successfully!")
        print(f"(Status code: {response.status_code})")

    test_telegram_connection()


telegram_bot_test()
