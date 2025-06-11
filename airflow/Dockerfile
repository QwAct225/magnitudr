FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies including DVC
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install DVC with Google Drive support
RUN pip install --no-cache-dir 'dvc'

# Copy project files
COPY operators /opt/airflow/operators
COPY utils /opt/airflow/utils
COPY dags /opt/airflow/dags

WORKDIR /opt/airflow
