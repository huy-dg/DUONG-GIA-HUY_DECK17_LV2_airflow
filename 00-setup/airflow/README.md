## Overview

Hướng dẫn này giúp bạn cài đặt Airflow bằng Docker

## 1. Create network

```shell
docker network create streaming-network --driver bridge
```

## 2. Initializing Environment

### 2.1 Setting the right Airflow user

Tạo các thư mục sau: `dags`, `logs`, `plugins`, `config`

```shell
mkdir -p ./dags ./logs ./plugins ./config
```

Lấy thông tin user id sử dụng lệnh sau:

```shell
id -u
```

và group id của group `docker` sử dụng lệnh sau:

```shell
getent group docker
```

Set thông tin thu được vào 2 biến `AIRFLOW_UID` và `DOCKER_GID` trong file `.env`

### 2.2 Initialize the database

```shell
docker compose up airflow-init
```

## 3. Running Airflow

```shell
docker compose up -d
```

**Check Status**

```shell
docker compose ps
```

## 4. Accessing the web interface

[web interface](http://localhost:18080)

username/password: `airflow/airflow`

## References

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)