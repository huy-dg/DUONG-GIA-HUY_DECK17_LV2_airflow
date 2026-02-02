from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.exceptions import AirflowException, AirflowSkipException
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
from confluent_kafka.admin import OffsetSpec

TARGET_TOPIC = "product_view"


@dag(
    dag_id="test_offset_spec",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["kafka", "test"],
)
def test_offset_spec():
    @task(task_id="test")
    def test():
        admin_client = KafkaAdminClientHook(kafka_config_id="kafka_default").get_conn
        topics_metadata = admin_client.list_topics(timeout=10).topics
        partitions_list = topics_metadata[TARGET_TOPIC].partitions.keys()
        tp_list = [TopicPartition(TARGET_TOPIC, p) for p in partitions_list]
        offset_spec = OffsetSpec.latest()
        tp_offsets = {tp: offset_spec for tp in tp_list}
        tp_futures = admin_client.list_offsets(tp_offsets)
        futures = tp_futures.values()
        log_end_offset_list = [future.result().offset for future in futures]
        print(list(partitions_list))
        print(log_end_offset_list)

    test()


test_offset_spec()
