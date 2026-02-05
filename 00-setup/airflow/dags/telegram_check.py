from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import requests
import json
import html


@dag(
    dag_id="telegram_bot_alert",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
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
        triggerer = conf.get("triggerer", "Manual")
        message = conf.get("message", '{"nodeUsage":{"min":"0.39%","median":"0.65%","max":"0.65%","stdDev":"0.13%"}}')
        short_msg = (
            json.dumps(message["hdfs"]["node_usage"], indent=2)
            if isinstance(message, dict) and "hdfs" in message
            else message
        )
        safe_json = html.escape(short_msg)
        print(f"{triggerer} - {short_msg}")
        print(f"{type(triggerer)} - {type(short_msg)}")
        conn = BaseHook.get_connection("telegram_bot")
        payload = conn.get_extra_dejson()

        text_content = (
	    f"🚀 <b>AIRFLOW CLUSTER REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Time:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"🏷 <b>Task:</b> <i>Health Check Status</i>\n"
            f"👤 <b>Triggerer:</b> <u>{conf.get('triggerer', 'Manual')}</u>\n\n"
            f"📊 <b>Node Usage Detail:</b>\n"
            f"<pre>{safe_json}</pre>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <i>Status: Monitoring Active</i>"
        )
        payload.update(
            {
                "text": text_content,
                "parse_mode": "HTML"
            }
        )
        response = requests.post(
            url=conn.host + "bot" + conn.password + "/sendMessage",
            json=payload,
            timeout=10,
        )
        print(f"Connection established: {response.status_code}")
        response.raise_for_status()
        print("Message sent successfully!")
        print(f"(Status code: {response.status_code})")

    send_message()


telegram_bot_alert()
