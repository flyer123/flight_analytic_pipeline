FROM apache/airflow:2.9.1

USER root

# Java for Spark
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && \
    apt-get clean

# Install Spark client
ENV SPARK_VERSION=3.5.0

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/

ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow