from datetime import datetime, timedelta
import requests

from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException, AirflowSkipException

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# TARGET_TABLE = "fact_view"
# FRESHNESS_THRESHOLD_HOURS = 24


@dag(
    dag_id="Spark_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "healthcheck"],
)
def spark_healthcheck():

    check_yarn_rm = HttpSensor(
        task_id="check_yarn_rm",
        http_conn_id="yarn_rm_conn",
        endpoint="/ws/v1/cluster/info",
        poke_interval=30,
        timeout=120,
    )

    @task(task_id="get_cluster_metadata")
    def get_cluster_metadata():
        # Gọi API của Resource Manager
        conn = BaseHook.get_connection("yarn_rm_conn")

        response = requests.get(conn.host + "ws/v1/cluster/nodes", timeout=10)
        response.raise_for_status()
        nodes_data = response.json().get("nodes", {}).get("node", [])

        worker_info = []
        for node in nodes_data:
            node_info = {
                "id": node["id"],
                "host": node["nodeHostName"],
                "state": node["state"],
                "memory_total": node["totalResource"]["memory"],
                "vcores_total": node["totalResource"]["vCores"],
                "memory_used": node["memUtilization"],
                "cpu_used": node["cpuUtilization"],
            }
            worker_info.append(node_info)
            print(f"Node Found: {node_info['host']} [{node_info['state']}]")

        # A. Lấy thông tin từ HDFS NameNode (LiveNodes, DeadNodes, PercentUsed)
        hdfs_hook = BaseHook.get_connection("hdfs_conn")
        hdfs_response = requests.get(
            hdfs_hook.host + "jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo",
            timeout=10,
        )
        hdfs_data = hdfs_response.json().get("beans", [{}])[0]

        # Giải mã JSON string trong LiveNodes/DeadNodes nếu cần
        live_nodes = hdfs_data.get("LiveNodes", "{}")
        dead_nodes = hdfs_data.get("DeadNodes", "{}")
        node_usage = hdfs_data.get("NodeUsage", "{}")

        return {
            "worker_nodes": worker_info,
            "hdfs": {
                "live_nodes": live_nodes,
                "dead_nodes": dead_nodes,
                "node_usage": node_usage,
            },
        }

    _cluster_metadata = get_cluster_metadata()

    # # 3. Check Data Freshness trong Postgres
    # # Kiểm tra xem có bản ghi nào mới được tạo trong X giờ qua không
    # # Trả về True nếu có dữ liệu (Success), False nếu không (sẽ retry/fail)
    # check_data_freshness = SqlSensor(
    #     task_id="check_postgres_data_freshness",
    #     conn_id="postgres_dw",
    #     sql=f"""
    #         SELECT 1
    #         FROM {TARGET_TABLE}
    #         WHERE updated_at >= NOW() - INTERVAL '{FRESHNESS_THRESHOLD_HOURS} HOURS'
    #         LIMIT 1;
    #     """,
    #     poke_interval=60,  # Check mỗi 60s
    #     timeout=60 * 5,  # Timeout sau 5 phút nếu data chưa về
    #     mode="reschedule",  # Giải phóng worker slot khi chờ đợi
    # )

    # # 4. (Optional) Check Data Quality cơ bản
    # # Ví dụ: Không được phép có giá trị null ở cột quan trọng
    # check_data_quality = SqlSensor(
    #     task_id="check_null_values",
    #     conn_id="postgres_dw",
    #     sql=f"""
    #         SELECT 1 AS status
    #         HAVING (SELECT COUNT(*) FROM {TARGET_TABLE} WHERE transaction_id IS NULL) = 0;
    #     """,
    #     fail_on_empty=True,  # Fail nếu query trả về rỗng (nghĩa là có NULL)
    #     poke_interval=60,
    #     timeout=120,
    # )

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_telegram_alert",
        trigger_dag_id="telegram_bot_alert",
        conf={"triggerer": "spark_healthcheck", "message": _cluster_metadata},
        trigger_rule="all_done",
        wait_for_completion=True,
        poke_interval=10,
    )

    check_yarn_rm >> _cluster_metadata >> trigger_alert


# >> [check_data_freshness, check_data_quality]


spark_healthcheck()
