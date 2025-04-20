## Overview

Phần hướng dẫn này bao gồm:

- Cách tạo một `http connection` trên airflow
- Sử dụng connection này trong `http operator` để get dữ liệu từ một rest api

## 1. Tạo http connection

Thêm connection vào trong file [airflow_settings.yaml](../00-setup/airflow/airflow_settings.yaml).

Thêm vào trong phần `airflow.connections` thông tin http connection như sau:

```
- conn_id: user_api
  conn_type: http
  conn_host: https://randomuser.me/
  conn_schema:
  conn_login:
  conn_password:
  conn_port:
  conn_extra:
```

Restart lại `airflow` để load lại thông tin `connection` mới.

```
astro dev restart
```

Truy cập vào web interface mục `Admin > Connections` sẽ có kết quả như sau:

![](img/http-connection.png)

## 2. Tạo http operator

Khai báo dag và http operator như trong file [user_processing.py](user_processing.py). Lưu ý ` http_conn_id='user_api'`
chính là `conn_id` đã set ở bước tạo connection

Tiếp theo bạn thêm file này vào trong thư mục `dags` và bật trên giao diện tương tự như bài tập `hello-airflow`.

Tại phần logs của task instance, bạn sẽ thấy response được in ra như sau:

![](img/http-response.png)