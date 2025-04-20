## Overview

Hướng dẫn này giúp bạn cài đặt Airflow sử dụng công cụ Astro CLI

## 1. Cài đặt Astro CLI

Bạn thực hiện cài đặt Astro CLI theo hướng dẫn trong link sau:

[Install Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

## 2. Cài đặt Airflow

### 2.1. Khởi tạo project

Tạo thư mục project:

```
mkdir airflow
```

Tại thư mục `airflow`, chạy lệnh sau:

```
astro dev init
```

### 2.2 Run Airflow

**Lưu ý: ** Bạn cần chạy các lệnh dưới đây tại thư mục project airflow mà bạn đã tạo ở bước `2.1`

#### 2.2.1 Configure the CLI

Vì port mặc định web interface của Airflow là `8080` trùng với port web interface của spark nên chúng ta cần cấu hình
lại thông tin này

```
astro config set webserver.port 8280
```

#### 2.2.2. Build & Run

Chạy lệnh sau (chú ý chạy tại thư mục `airflow`)

```
astro dev start
```

Lệnh này sẽ build project và start 4 docker containers sau:

- Postgres: Airflow's metadata database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for running Triggers and signaling tasks to resume when their conditions
  have been met. The triggerer is used exclusively for tasks that are run with deferrable operators.

#### 2.2.3. Accessing the web interface

Sau khi các container đã start thành công. Bạn truy cập vào giao diện web theo link sau:

[web interface](https://localhost:8280/)

username/password: `admin/admin`

## References

[https://www.astronomer.io/docs/astro/cli/get-started-cli](https://www.astronomer.io/docs/astro/cli/get-started-cli)

[https://www.astronomer.io/docs/astro/cli/configure-cli/](https://www.astronomer.io/docs/astro/cli/configure-cli/)