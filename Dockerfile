# Airflow 3.1 with VaultSpeed plugin (vs_fmc_plugin) support
FROM apache/airflow:3.1.8

USER airflow

# Base pip dependencies (Airflow pin, Snowflake, Databricks, etc.)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install VaultSpeed provider from plugins/vs_fmc_plugin (with Snowflake + Databricks extras)
# COPY as root then chown so airflow can write build artifacts (e.g. .egg-info) during pip install
USER root
COPY plugins/vs_fmc_plugin /opt/airflow/plugins/vs_fmc_plugin
RUN chown -R airflow:root /opt/airflow/plugins/vs_fmc_plugin
USER airflow
RUN pip install --no-cache-dir "/opt/airflow/plugins/vs_fmc_plugin[snowflake,databricks]"
