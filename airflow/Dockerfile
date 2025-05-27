FROM apache/airflow:2.7.3-python3.11

USER root

# Install libraries needed for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create directories with correct permissions
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed && \
    chown -R airflow:root /opt/airflow/data

# Copy and prepare entrypoint script
COPY --chown=airflow:root scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENTRYPOINT ["/entrypoint.sh"]