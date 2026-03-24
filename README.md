# Airflow 3.1 Docker image with VaultSpeed plugin

Docker image based on **Apache Airflow 3.1.8** with support for the **VaultSpeed** plugin, plus a ready-to-use Docker Compose stack (CeleryExecutor, PostgreSQL, Redis).

## Quick start

1. **Prepare environment (Linux: set your user ID so files aren’t owned by root)**

   ```bash
   mkdir -p ./dags ./logs ./plugins ./config
   cp .env.example .env
   # On Linux, set AIRFLOW_UID to your host user id:
   echo "AIRFLOW_UID=$(id -u)" >> .env
   ```

2. **Build the custom image**

   ```bash
   docker compose build
   ```

3. **Initialize Airflow (first time only)**

   ```bash
   docker compose up airflow-init
   ```

   Default admin: **airflow** / **airflow**.

4. **Start the stack**

   ```bash
   docker compose up -d
   ```

5. **Open the UI**

   - API/UI: http://localhost:8080  
   - Flower (optional): `docker compose --profile flower up -d` then http://localhost:5555  

## VaultSpeed plugin

The **VaultSpeed FMC provider** (`airflow-provider-vaultspeed`) is installed from `plugins/vs_fmc_plugin` when the image is built. It is an Airflow provider that registers:

- **Hooks:** Spark SQL, Livy, SingleStore, dbt CLI, dbt Cloud  
- **Operators:** Databricks, BigQuery, dbt, JDBC-to-JDBC, Spark SQL, and others  

The image installs it with the **Snowflake** and **Databricks** extras. To add more extras (e.g. `jdbc`, `google`), edit the Dockerfile line that runs `pip install -e "…[snowflake,databricks]"` and add the extras you need.

The same `plugins/vs_fmc_plugin` directory is mounted at runtime, so you can update the plugin code on the host and rebuild the image to pick up changes.

## Project layout

| Path              | Purpose                    |
|-------------------|----------------------------|
| `Dockerfile`      | Extends `apache/airflow:3.1.8`, installs `requirements.txt` and `plugins/vs_fmc_plugin` (VaultSpeed provider) |
| `requirements.txt` | Pip deps (Airflow pin, Snowflake, Databricks, optional HashiCorp) |
| `docker-compose.yaml` | Full Airflow 3.1 stack (scheduler, worker, triggerer, API server, Postgres, Redis) |
| `dags/`           | Your DAGs (mounted into the container) |
| `plugins/`        | Custom Airflow plugins; `plugins/vs_fmc_plugin` is the VaultSpeed provider (baked into the image) |
| `config/`         | Optional `airflow.cfg` / settings |
| `logs/`           | Task and scheduler logs    |

## Useful commands

```bash
# Run Airflow CLI in a service
docker compose run airflow-worker airflow info

# Stop and remove containers + DB volume
docker compose down --volumes --remove-orphans
```

## Requirements

- Docker and Docker Compose v2.14+
- At least 4 GB RAM for Docker (8 GB recommended on macOS)

This setup is intended for **local development and try-out**. For production, use a proper deployment (e.g. [Airflow Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)).
