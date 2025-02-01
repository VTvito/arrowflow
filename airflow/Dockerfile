# Usa l'immagine ufficiale di Apache Airflow
FROM apache/airflow:latest

# Imposta le variabili di ambiente per il database e Prometheus
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__METRICS__STATSD_ON=True
ENV AIRFLOW__METRICS__STATSD_HOST=statsd-exporter
ENV AIRFLOW__METRICS__STATSD_PORT=9125

# Crea le cartelle necessarie
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Set Python path to "see" the praparator module 
ENV PYTHONPATH="/opt/airflow/"

# CMD per avvio servizi
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]