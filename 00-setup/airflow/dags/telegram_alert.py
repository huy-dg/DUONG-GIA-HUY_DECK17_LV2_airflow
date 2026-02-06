from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import requests
import json
import html


@dag(
    dag_id="telegram_bot_alert",
    schedule=None,
    catchup=False,
    tags=["alert", "telegram"],
)
def telegram_bot_alert():
    @task(
        task_id="telegram_bot_alert",
    )
    def send_message():
        context = get_current_context()
        conf = context["dag_run"].conf
        triggerer = conf.get("triggerer", "Self-test")
        message = conf.get("message", "Test message from Airflow.")
        safe_json = html.escape(message)
        print(f"{triggerer} - {message}")
        print(f"{type(triggerer)} - {type(message)}")

        conn = BaseHook.get_connection("telegram_bot")
        payload = conn.get_extra_dejson()
        text_content = (
            f"🚀 <b>AIRFLOW CLUSTER REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Time:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"🏷 <b>Task:</b> <i>Health Check Status</i>\n"
            f"👤 <b>Triggerer: {triggerer.upper()}</b>\n"
            f"📊 <b>Cluster Detail:</b>\n"
            f"<pre>{safe_json}</pre>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <i>Status: Monitoring Active</i>"
        )
        payload.update({"text": text_content, "parse_mode": "HTML"})
        response = requests.post(
            url=conn.host + "bot" + conn.password + "/sendMessage",
            json=payload,
            timeout=10,
        )
        print(f"Connection established: {response.status_code}")
        response.raise_for_status()

        print("Message sent successfully!")

    send_message()


telegram_bot_alert()
