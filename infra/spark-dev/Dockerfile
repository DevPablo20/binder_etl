# Bookworm still ships OpenJDK 17; current python:3.11-slim (Trixie) only has 21.
FROM python:3.11-slim-bookworm

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/spark.txt /tmp/spark.txt
RUN pip install --no-cache-dir -r /tmp/spark.txt

WORKDIR /opt/binder_etl

ENV PYTHONPATH=/opt/binder_etl
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

CMD ["sleep", "infinity"]
