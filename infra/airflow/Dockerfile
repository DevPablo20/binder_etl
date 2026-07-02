FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements/airflow.txt requirements/spark.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/airflow.txt -r /tmp/spark.txt

ENV PYTHONPATH=/opt/binder_etl
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
