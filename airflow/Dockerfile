FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY operators /opt/airflow/operators
COPY utils /opt/airflow/utils
COPY dags /opt/airflow/dags

WORKDIR /opt/airflow
