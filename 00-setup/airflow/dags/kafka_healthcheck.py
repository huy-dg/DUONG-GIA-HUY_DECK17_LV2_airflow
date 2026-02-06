from datetime import datetime, timedelta
import requests

from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
from confluent_kafka.admin import OffsetSpec
from include.on_failure_callback import trigger_alert_on_failure

KAFKA_CONN_ID = "kafka_default"
TARGET_TOPIC = "product_view"
DEFAULT_CONSUMER_GROUP_ID = "product_reader_group"
OFFSET_LAG_THRESHOLD = 10000
THROUGHPUT_THRESHOLD = 1500


@dag(
    dag_id="Kafka_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    on_failure_callback=trigger_alert_on_failure,
    catchup=False,
    tags=["kafka", "healthcheck"],
)
def kafka_healthcheck():

    @task(task_id="connection_task")
    def test_connection():
        is_connected, message = KafkaAdminClientHook(
            kafka_config_id=KAFKA_CONN_ID
        ).test_connection()
        if not is_connected:
            raise AirflowException(f"Kafka Connection Failed: {message}")

        print(message)
        return is_connected

    @task(task_id="metadata_task")
    def retrieve_metadata():
        """
        Docstring for retrieve_metadata
        :return: Description
        :rtype: dict[Any, Any]

        function flow:
        Create Kafka AdminClient connection <- KafkaAdminClientHook
                V
        List topics metadata <- list_topics() method
                V
        Verify TARGET_TOPIC existence
                V
        List consumer groups <- list_consumer_groups() method
                V
        Assign consumer group to monitor
                V
        Create TopicPartition list obj, <- TARGET_TOPIC, partitions list
                V
        Create ConsumerGroupTopicPartitions obj <- consumer_group_id, TopicPartition list
                V
        Retrieve committed offsets of consumer group <- list_consumer_group_offsets() method
                V
        Retrieve log end offsets <- list_offsets() method
                V
        Combine partition metadata and return

        """
        admin_client = KafkaAdminClientHook(kafka_config_id=KAFKA_CONN_ID).get_conn
        print("Start retrieving metadata...")

        topics_metadata = admin_client.list_topics(timeout=10).topics
        existing_topics_list = topics_metadata.keys()

        if TARGET_TOPIC not in existing_topics_list:
            raise AirflowException(
                f"Topic {TARGET_TOPIC} does not exist in the cluster."
            )
        print(f"Topic '{TARGET_TOPIC}' found in the cluster.")

        partitions_list = topics_metadata[TARGET_TOPIC].partitions.keys()
        tp_list = [TopicPartition(TARGET_TOPIC, p) for p in partitions_list]

        # Determine consumer group to monitor
        consumer_groups_result = admin_client.list_consumer_groups().result()
        groups_id_list = [group.group_id for group in consumer_groups_result.valid]
        if DEFAULT_CONSUMER_GROUP_ID in groups_id_list:
            consumer_group_id = DEFAULT_CONSUMER_GROUP_ID
        else:
            consumer_group_id = groups_id_list[0]
        print(f"Assign consumer group '{consumer_group_id}' to monitor.")

        # Retrieve committed offsets of assigned consumer group
        consumer_tp_request = ConsumerGroupTopicPartitions(consumer_group_id, tp_list)
        dict_futures = admin_client.list_consumer_group_offsets([consumer_tp_request])
        consumer_tp_future = dict_futures[consumer_group_id]
        consumer_tp_obj = consumer_tp_future.result()
        consumer_tp_list = [
            consumer_tp for consumer_tp in consumer_tp_obj.topic_partitions
        ]
        partition_metadata = {
            tp.partition: {"committed_offset": tp.offset} for tp in consumer_tp_list
        }

        # Retrieve log end offsets
        offset_spec = OffsetSpec.latest()
        tp_offsets = {tp: offset_spec for tp in tp_list}
        futures = admin_client.list_offsets(tp_offsets).values()
        log_end_offset_list = [future.result().offset for future in futures]
        if len(log_end_offset_list) == len(partition_metadata):
            for key, value in zip(partition_metadata.keys(), log_end_offset_list):
                partition_metadata[key]["log_end_offset"] = value
            print("Partition metadata: ")
            [
                print(f"partition {partition_id}: {metadata}")
                for partition_id, metadata in partition_metadata.items()
            ]
        else:
            raise AirflowException(
                "Mismatch in number of partitions and log end offsets retrieved."
            )

        print("Metadata retrieval completed.")
        return {
            "target_topic_name": TARGET_TOPIC,
            "all_groups": groups_id_list,
            "consumer_group_id": consumer_group_id,
            "partition_metadata": partition_metadata,
        }

    _retrieve_metadata = retrieve_metadata()

    @task(task_id="metrics_task")
    def calculate_metrics(topic_metadata) -> dict:
        """
        Docstring for calculate_metrics

        :param topic_metadata: Description
        :type topic_metadata: dict[Any, Any]
        :return: Description
        :rtype: dict[Any, Any]
        """
        target_topic = topic_metadata["target_topic_name"]
        partitions_metadata = topic_metadata["partition_metadata"]
        total_offset_lag = 0
        partition_metrics_map = {}

        for pid, metadata in partitions_metadata.items():
            log_end_offset = metadata["log_end_offset"]
            committed_offsets = metadata["committed_offset"]
            partition_key = pid

            partition_is_empty = log_end_offset == committed_offsets
            if partition_is_empty:
                current_lag = 0
            else:
                current_lag = (
                    log_end_offset - committed_offsets
                    if committed_offsets >= 0
                    else log_end_offset
                )

            total_offset_lag += current_lag
            partition_metrics_map[partition_key] = {
                "log_end_offset": log_end_offset,
                "committed_offset": committed_offsets,
                "current_lag": current_lag,
                "partition_is_empty": partition_is_empty,
            }

        total_committed_offsets = sum(
            partition_metrics["committed_offset"]
            for partition_metrics in partition_metrics_map.values()
            if partition_metrics["committed_offset"] >= 0
        )
        task_timestamp = timezone.utcnow().timestamp()

        print(
            f"Metrics for group_id {topic_metadata['consumer_group_id']} on topic {target_topic}: "
        )
        print(f"  -  Total lag: {total_offset_lag}")
        return {
            "monitor_group_id": topic_metadata["consumer_group_id"],
            "total_lag": total_offset_lag,
            "partition_metrics": partition_metrics_map,
            "total_consumed_msg": total_committed_offsets,
            "metric_task_ts": task_timestamp,
        }

    _calculate_metrics = calculate_metrics(_retrieve_metadata)

    @task(task_id="msg_throughput_task")
    def calculate_throughput(current_metrics, **context) -> dict[str, float]:
        ti = context["ti"]
        prev_metrics = ti.xcom_pull(task_ids="metrics_task", include_prior_dates=True)

        if not prev_metrics or "total_consumed_msg" not in prev_metrics:
            print("This is the first DAG run, no throughput data yet.")
            return {"throughput_rate": 0}

        delta_consumed_msg = (
            current_metrics["total_consumed_msg"] - prev_metrics["total_consumed_msg"]
        )
        delta_timestamp = (
            current_metrics["metric_task_ts"] - prev_metrics["metric_task_ts"]
        )
        if delta_timestamp > 0 and delta_consumed_msg >= 0:
            throughput = delta_consumed_msg / delta_timestamp
        else:
            print(
                "Time between 2 DAG runs are too close or \
                Consumer Group offsets were reset to 0, \
                consider increase DAG schedule_interval."
            )
            throughput = 0

        print(
            f"Processed {delta_consumed_msg} messages in {delta_timestamp:.1f} seconds."
        )
        print(f"🚀 Throughput: {throughput:.2f} messages/sec")

        return {"throughput_rate": throughput}

    _calculate_throughput = calculate_throughput(_calculate_metrics)

    @task(task_id="healthcheck_task")
    def threshold_check(metrics, throughput):
        current_total_lag = metrics["total_lag"]
        # partition_metrics = metrics["partition_metrics"]
        current_throughput = throughput["throughput_rate"]
        consumer_group_id = metrics["monitor_group_id"]
        alert_messages = []

        if current_total_lag == 0:
            print("✅ All good, no lag detected.")
            raise AirflowSkipException("Metrics within normal limits.")

        if current_total_lag > OFFSET_LAG_THRESHOLD:
            alert_messages.append(
                f"Consumer is lagging behind to much: {current_total_lag}."
            )
        if current_total_lag > 0 and current_throughput < THROUGHPUT_THRESHOLD:
            alert_messages.append(
                f"Throughput is below threshold: {current_throughput:.2f} msg/sec."
            )

        if current_throughput > 0:
            recovery_period = round((current_total_lag / current_throughput) / 60, 1)
        else:
            recovery_period = "N/A"

        message = (
            f"📦 *Topic:* `{TARGET_TOPIC}`\n"
            f"👥 *Consumer Group:* `{consumer_group_id}`\n"
            f"📉 *Current Lag:* `{current_total_lag:,.0f}`\n"
            f"🚀 *Throughput:* `{current_throughput:.2f} msg/s`\n"
            f"⏳ *Est. Recovery:* `{recovery_period} mins`\n"
            f"----------------------------\n"
            f"❌ *Issue Detected:*\n{alert_messages}"
        )
        return message

    _threshold_message = threshold_check(_calculate_metrics, _calculate_throughput)

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_telegram_alert",
        trigger_dag_id="telegram_bot_alert",
        conf={"triggerer": "kafka_healthcheck", "message": _threshold_message},
        # trigger_rule="all_done",
        wait_for_completion=False,
        poke_interval=10,
    )

    (
        test_connection()
        >> _retrieve_metadata
        >> _calculate_metrics
        >> _calculate_throughput
        >> _threshold_message
        >> trigger_alert
    )


kafka_healthcheck()
