# Copilot Instructions - Airflow Project

## Project Overview
This is an Apache Airflow learning/practice project demonstrating data pipeline orchestration patterns. The project includes a Docker-based development environment with PostgreSQL backend and multiple example DAGs showing different Airflow features.

**Key Technologies:** Apache Airflow 2.5.1, PostgreSQL, Docker, Kafka, Spark

## Architecture

### Core Components
- **`00-setup/airflow/`**: Production-ready Docker environment with Airflow services
  - `dags/`: All DAG definitions (main workflow code)
  - `config/`: Airflow configuration (`airflow.cfg`)
  - `plugins/`: Custom hooks and operators (see `hooks/` and `user_report_hook.py`)
  - `docker-compose.yaml`: Multi-service setup (webserver, scheduler, postgres, redis)

- **Numbered Directories (01-12)**: Progressive examples of Airflow features with corresponding DAGs in `00-setup/airflow/dags/`

### Data Flow Patterns
1. **Simple Python Tasks**: `hello_airflow.py` - basic `PythonOperator` with no I/O
2. **API → CSV → Database**: Most DAGs extract from dummy API, process to CSV, store to PostgreSQL
3. **Sensor-Driven**: `HttpSensor` validates API availability before extraction tasks
4. **Branching**: `branching_user_processing.py` uses `BranchPythonOperator` for conditional paths
5. **XCom Data Passing**: Tasks use `ti.xcom_push()` / `ti.xcom_pull()` for inter-task communication
6. **Task Groups**: Logical grouping of related tasks (see `tg_user_processing.py`)
7. **Dynamic Task Mapping**: `dynamic_user_processing.py` generates tasks based on pagination
8. **Dataset-Based Scheduling**: DAGs triggered by upstream file/data changes

## Developer Workflows

### Setup & Running
```bash
cd 00-setup/airflow

# Build custom Docker image
docker build -t unigap/airflow:2.10.4 .

# Create Docker network
docker network create streaming-network --driver bridge

# Start services
docker compose up -d

# Access Airflow UI: http://localhost:8080 (airflow/airflow)
```

### Key Commands
- **View DAG logs**: `docker compose logs -f airflow-scheduler` (real-time scheduler logs)
- **Execute task manually**: Use Airflow UI → Trigger DAG → check task logs
- **Database access**: `docker compose exec postgres psql -U airflow -d airflow`
- **Rebuild image after dependency changes**: `docker compose up --build`

### Adding New DAGs
1. Create `.py` file in `00-setup/airflow/dags/`
2. Define DAG with `dag_id` parameter (must be unique)
3. File is auto-discovered; restart scheduler or wait for DAG parsing
4. Configure connections in Airflow UI before running (e.g., `dummy_api`, `postgres_default`)

## Project-Specific Patterns & Conventions

### Connection Configuration
Connections are configured through Airflow UI or environment variables. Common connections:
- `dummy_api`: REST API endpoint (HOST required)
- `postgres_default` or `postgres`: PostgreSQL database
- `kafka_default`: Kafka broker connection
- Accessed via `BaseHook.get_connection("conn_id")`

### DAG Definition Patterns
**Style 1 - Context Manager (Classic)**
```python
with DAG("dag_id", start_date=..., schedule_interval="@daily") as dag:
    task1 = PythonOperator(...)
    task1 >> task2
```

**Style 2 - Decorator (Modern, preferred for cleaner code)**
```python
@dag(dag_id="dag_id", start_date=..., schedule_interval="@daily")
def my_dag():
    @task
    def my_task():
        return "result"
    return my_task()
```

### Common Task Patterns
- **External API calls**: Always precede with `HttpSensor` to check availability
- **CSV processing**: Use `pandas.json_normalize()` for data transformation
- **Database operations**: Use `PostgresHook.copy_expert()` for efficient bulk inserts
- **Kafka integration**: Use `KafkaAdminClientHook` + `KafkaConsumerHook` for health checks

### File Locations
- Temporary processing: `/tmp/processed_user.csv` (shared across tasks via filesystem)
- XCom for small data: Task instance context (`ti.xcom_push/pull`)
- Database for persistence: PostgreSQL `users` table (commonly used for user data)

## Cross-Component Communication
- **Between Tasks**: XCom (via `ti` parameter in `PythonOperator`)
- **External Services**: HTTP connections (dummy API at `http://dummy-api:5000/`), PostgreSQL, Kafka
- **Airflow Config**: Connections stored in PostgreSQL backend; plugins auto-loaded from `plugins/` directory
- **Custom Hooks**: Placed in `plugins/hooks/` (e.g., `UserReportHook` extends `BaseHook`)

## Critical Dependencies
- **Apache Airflow Providers**:
  - `apache-airflow-providers-postgres`: Database operations
  - `apache-airflow-providers-docker`: Container execution
  - `apache-airflow-providers-apache-kafka`: Kafka integration
- **Data Libraries**: `pandas` (transformations), `psycopg[binary]` (PostgreSQL driver)
- **External Services**: Dummy API must be running (check via `HttpSensor`)

## Important Notes
- DAGs must have unique `dag_id` values across the project
- Airflow scheduler parses DAG files automatically; changes take effect within minutes
- PostgreSQL must be initialized before DAGs can store data
- Docker network `streaming-network` must exist for multi-container communication
- Logs are stored in `00-setup/airflow/logs/` by DAG ID and run timestamp

## Code Examples by Feature
- **Scheduling**: `hello_airflow.py` (cron), `kafka_healthcheck.py` (@daily)
- **Branching**: `branching_user_processing.py` (BranchPythonOperator)
- **Datasets**: `dataset_user_processing.py` (Dataset-based trigger)
- **Task Groups**: `tg_user_processing.py` (TaskGroup organization)
- **Dynamic Mapping**: `dynamic_user_processing.py` (expand mapped tasks)
- **Plugins**: `12-plugins/user_reporting.py` + `user_report_hook.py` (custom hooks)
