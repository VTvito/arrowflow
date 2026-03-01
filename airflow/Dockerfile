# Official Apache Airflow image (pinned for reproducibility)
FROM apache/airflow:2.10.4

# Metrics: export Airflow stats to Prometheus via statsd-exporter
# DB connection string is injected at runtime by docker-compose (see AIRFLOW__DATABASE__SQL_ALCHEMY_CONN)
ENV AIRFLOW__METRICS__STATSD_ON=True
ENV AIRFLOW__METRICS__STATSD_HOST=statsd-exporter
ENV AIRFLOW__METRICS__STATSD_PORT=9125

# Crea le cartelle necessarie
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Set Python path to "see" the praparator module 
ENV PYTHONPATH="/opt/airflow/"

# CMD per avvio servizi (db migrate ensures schema exists on first boot)
# Creates admin user on first boot only (idempotent: skips if already exists)
CMD ["bash", "-c", "airflow db migrate && (airflow users list | grep -q admin || airflow users create --username admin --password admin --firstname Admin --lastname ETL --role Admin --email admin@etl.local) && (airflow webserver & airflow scheduler)"]