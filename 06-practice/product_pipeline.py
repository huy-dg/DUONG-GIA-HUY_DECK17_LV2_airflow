import json
import csv
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize


def _process_products(ti):
    extracted_data = ti.xcom_pull(task_ids="extract_products")
    products_list = extracted_data["products"]
    df = json_normalize(products_list)
    processed_products = df[
        [
            "id",
            "title",
            "description",
            "category",
            "price",
            "discountPercentage",
            "rating",
            "stock",
            "brand",
            "sku",
            "weight",
            "dimensions.width",
            "dimensions.height",
            "dimensions.depth",
            "warrantyInformation",
            "shippingInformation",
            "availabilityStatus",
            "returnPolicy",
            "minimumOrderQuantity",
            "images",
        ]
    ].copy()
    processed_products.columns = [
        "id",
        "title",
        "description",
        "category",
        "price",
        "discount_percentage",
        "rating",
        "stock",
        "brand",
        "sku",
        "weight",
        "width",
        "height",
        "depth",
        "warranty_information",
        "shipping_information",
        "availability_status",
        "return_policy",
        "minimum_order_quantity",
        "images",
    ]

    processed_products.to_csv(
        "/tmp/processed_products.csv", index=False, header=False, quoting=csv.QUOTE_ALL
    )


def _store_products():
    hook = PostgresHook(postgres_conn_id="postgres", database="airflow")

    hook.copy_expert(
        sql="COPY products FROM stdin WITH (FORMAT CSV, QUOTE '\"', DELIMITER ',')",
        filename="/tmp/processed_products.csv",
    )


with DAG(
    "products_processing",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="dummy_api",
        endpoint="products?limit=10",
        timeout=60,
        poke_interval=20,
    )

    extract_products = HttpOperator(
        task_id="extract_products",
        http_conn_id="dummy_api",
        endpoint="products?limit=0",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    process_products = PythonOperator(
        task_id="process_products", python_callable=_process_products
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        database="airflow",
        sql="""
        CREATE TABLE IF NOT EXISTS products (
            id BIGINT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            discount_percentage NUMERIC(5,2) NOT NULL,
            rating NUMERIC(4,2) NOT NULL,
            stock INTEGER NOT NULL,
            brand TEXT NOT NULL,
            sku TEXT NOT NULL,
            weight NUMERIC(5,2) NOT NULL,
            width NUMERIC(5,2) NOT NULL,
            height NUMERIC(5,2) NOT NULL,
            depth NUMERIC(5,2) NOT NULL,
            warranty_information TEXT NOT NULL,
            shipping_information TEXT NOT NULL,
            availability_status TEXT NOT NULL,
            return_policy TEXT NOT NULL,
            minimum_order_quantity INTEGER NOT NULL,
            images TEXT NOT NULL
        );
        """,
    )

    store_products = PythonOperator(
        task_id="store_products", python_callable=_store_products
    )

    (
        is_api_available
        >> extract_products
        >> process_products
        >> create_table
        >> store_products
    )
