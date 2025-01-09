from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
        dag_id="busybox_container_dag",
        schedule="*/2 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    run_busybox_container = DockerOperator(
        task_id='run_busybox_container',
        image='busybox:1.37.0',
        container_name='airflow-busybox',
        auto_remove='force',
        command=["sh", "-c", "nc -z -v -w 2 113.160.15.232 9094 && echo '1' || echo '0'"],
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge"
    )
