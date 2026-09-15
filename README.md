# IoT Sensor ETL — Airflow DAG

A data-engineering pipeline built with **Apache Airflow 3.3.1** that runs a
daily ETL over simulated IoT sensor readings: **extract → transform → validate → load → summarize**.

## What it does

The DAG `iot_sensor_etl` (`schedule="@daily"`) processes ten simulated sensor
payloads on every run:

1. **Extract** – generates ten sample readings (`sensor_id`, `timestamp`, `temperature`, `humidity`).
2. **Transform** – computes temperature in Fahrenheit and normalizes the payload for the warehouse.
3. **Validate** – flags readings outside sane ranges (temperature 0–50 °C, humidity 0–100 %).
4. **Load** – prints each valid reading, simulating a write to a data warehouse.
5. **Summarize** – prints total valid readings plus average temperature and humidity.

Dependency chain: `start → extract → transform → validate → load → summarize → end`.

## Repository layout

```
dags/iot_sensor_etl.py   # The DAG (Airflow 3 TaskFlow API)
docker-compose.yaml      # Airflow 3.3.1 stack: CeleryExecutor + Postgres + Redis
.gitignore               # exclusions (.env, logs/, __pycache__/, ...)
```

## Prerequisites

- Docker Desktop with the WSL2 backend.
- The custom Airflow image `airflow-local:3.3.1` — built once from a
  `Dockerfile` + `requirements.txt` (`pip install -r requirements.txt`, run as
  the `airflow` user). If the image is present, `docker compose up` picks it up;
  rebuild it with `docker compose up -d --build`.
- A `.env` file next to `docker-compose.yaml`:

  ```
  AIRFLOW_UID=1000
  FERNET_KEY=<your-key>
  ```

  `.env` is gitignored and never committed.

## Run it

```bash
docker compose up -d
```

- Web UI: **http://localhost:8080** — login `airflow` / `airflow`.
- Services: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`,
  `airflow-worker`, `airflow-triggerer`, `postgres`, `redis`.
- The DAG is created **paused** (`DAGS_ARE_PAUSED_AT_CREATION=true`); unpause it
  in the UI (or trigger it manually) to run the pipeline.
- Stop the stack (keeps the Postgres volume — add `-v` only if you want a fresh DB):

  ```bash
  docker compose down
  ```