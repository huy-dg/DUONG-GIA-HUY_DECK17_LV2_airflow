from datetime import datetime, timedelta
import requests
import json

from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from include.on_failure_callback import trigger_alert_on_failure

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# TARGET_TABLE = "fact_view"
# FRESHNESS_THRESHOLD_HOURS = 24


@dag(
    dag_id="Spark_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    on_failure_callback=trigger_alert_on_failure,
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
        nodemanagers_data = response.json().get("nodes", {}).get("node", [])

        nodemanagers_list = []
        for nodemanager in nodemanagers_data:
            """
            node_info = {
                "id": node["id"],
                "host": node["nodeHostName"],
                "state": node["state"],
                "memory_total": node["totalResource"]["memory"],
                "vcores_total": node["totalResource"]["vCores"],
                "memory_used": node["memUtilization"],
                "cpu_used": node["cpuUtilization"],
            }
            """
            nodemanagers_list.extend(
                [
                    f"id: {nodemanager['id']}",
                    f"host_name: {nodemanager['nodeHostName']}",
                    f"state: {nodemanager['state']}",
                ]
            )
            print(f"Node Found: {nodemanager['nodeHostName']} [{nodemanager['state']}]")

        # A. Lấy thông tin từ HDFS NameNode (LiveNodes, DeadNodes, PercentUsed)
        hdfs_hook = BaseHook.get_connection("hdfs_conn")
        hdfs_response = requests.get(
            hdfs_hook.host + "jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo",
            timeout=10,
        )
        hdfs_data = hdfs_response.json().get("beans", [{}])[0]

        # Giải mã JSON string trong LiveNodes/DeadNodes nếu cần
        live_nodes = hdfs_data.get("LiveNodes", "{}")
        datanodes_list = []
        for data_node, node_info in json.loads(live_nodes).items():
            datanodes_list.extend(
                [
                    f"node_name: {data_node}",
                    f"node info: {node_info['infoAddr']}",
                    f"admin_state: {node_info['adminState']}",
                ]
            )
        dead_nodes = hdfs_data.get("DeadNodes", "{}")
        dead_nodes_str = (
            dead_nodes if len(json.loads(dead_nodes)) > 0 else "No dead nodes."
        )
        node_usage = json.loads(hdfs_data.get("NodeUsage", "{}"))
        node_usage_str = json.dumps(node_usage["nodeUsage"]).strip("{}")

        return {
            "manager_nodes": "\n".join(nodemanagers_list),
            "hdfs": {
                "data_nodes": "\n".join(datanodes_list),
                "dead_nodes": dead_nodes_str,
                "node_usage": node_usage_str,
            },
        }

    _cluster_metadata = get_cluster_metadata()

    @task(task_id="gate_check")
    def gate_check(cluster_metadata):
        manager_nodes = cluster_metadata["manager_nodes"]
        data_nodes = cluster_metadata["hdfs"]["data_nodes"]
        dead_nodes = cluster_metadata["hdfs"]["dead_nodes"]
        node_usage = cluster_metadata["hdfs"]["node_usage"]

        alert_messages = "No alerts."

        passed_message = (
            f"📦 Data nodes: \n{data_nodes}\n"
            f"💀 Dead nodes: \n{dead_nodes}\n"
            f"📉 Nodes usage: \n{node_usage}\n"
            f"👥 Manager nodes: \n{manager_nodes}\n"
            f"----------------------------\n"
            f"⚠️ Issue Detected: \n{alert_messages}"
        )
        return passed_message

    _passed_message = gate_check(_cluster_metadata)

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_telegram_alert",
        trigger_dag_id="telegram_bot_alert",
        conf={
            "triggerer": "spark_healthcheck",
            "message": _passed_message,
        },
        # trigger_rule="all_done",
        wait_for_completion=False,
        poke_interval=10,
    )

    check_yarn_rm >> _cluster_metadata >> _passed_message >> trigger_alert


# >> [check_data_freshness, check_data_quality]


spark_healthcheck()
