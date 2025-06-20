FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz -o spark.tgz \
    && tar -xzf spark.tgz \
    && mv spark-3.4.0-bin-hadoop3 /opt/spark \
    && rm spark.tgz \
    && chown -R airflow:root /opt/spark

USER airflow

# Copy and install requirements first
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install seaborn matplotlib great-expectations==0.17.19 scikit-learn

# Copy project files after dependencies are installed
COPY operators /opt/airflow/operators
COPY utils /opt/airflow/utils
COPY dags /opt/airflow/dags

WORKDIR /opt/airflow