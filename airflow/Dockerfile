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
CMD ["bash", "-c", "airflow db migrate && (airflow webserver & airflow scheduler)"]