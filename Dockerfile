FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.1}
USER airflow
RUN pip install apache-airflow-providers-airbyte[http] \
&& pip install apache-airflow-providers-airbyte \
&& pip install apache-airflow-providers-amazon