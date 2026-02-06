from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def trigger_alert_on_failure(context):
    """
    On failure callback to trigger the telegram_bot_alert DAG with failed DAG and task info.
    """
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    message = (
        f"⚠️ ALERT!\n"
        f"{dag_id} DAG has failed.\n"
        f"failed task: {task_id}.\n"
        f"Please check the Airflow UI for more details."
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_alert_task",
        trigger_dag_id="telegram_bot_alert",
        conf={"triggerer": dag_id, "message": message},
    )
    # Thực thi trigger
    trigger.execute(context=context)
