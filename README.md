# Magnitudr: Earthquake Data Analysis Platform

Magnitudr is an end-to-end platform for earthquake data analysis, covering data ingestion, processing, clustering, and visualization. It uses a container-based architecture with Docker to orchestrate various services, including Apache Airflow for data pipelines, Apache Spark for processing, FastAPI for serving an API, and Streamlit for an interactive dashboard.

## Architecture

The project consists of several key services managed by `docker-compose.yml`:

-   **`postgres`**: The primary database used by Apache Airflow to store its metadata.
-   **`airflow-webserver`**: The Airflow web UI, where you can monitor and manage DAGs.
-   **`airflow-scheduler`**: The Airflow component responsible for scheduling and triggering tasks.
-   **`airflow-worker`**: The Airflow component that executes the scheduled tasks.
-   **`airflow-init`**: A one-time service that initializes the Airflow database on the first run.
-   **`api`**: A FastAPI service that provides endpoints to access the processed data.
-   **`streamlit`**: An interactive web application for visualizing data and analysis results.

**Note on Spark:** Apache Spark is installed directly within the Airflow services' environment (`Dockerfile` for Airflow). This setup allows Spark jobs to be executed as local processes from within the Airflow container, simplifying the architecture for local development without requiring a separate Spark cluster.

## Prerequisites

Before you begin, ensure you have the following software installed on your system:

-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)

## Getting Started

Follow these steps to run the entire application stack locally.

### 1. Clone the Repository

First, clone this repository to your local machine:

```bash
git clone https://github.com/QwAct225/magnitudr
cd magnitudr
```

### 2. Initial Setup (Airflow Initialization)

Airflow requires a one-time initialization to set up its database and create a default user.

Run the following command to build and start all services in detached mode (`-d`):

```bash
docker-compose up --build -d
```

This process might take a few minutes on the first run as Docker needs to download images and build your containers.

Once all containers are running, you can verify their status with:

```bash
docker-compose ps
```

You should see a list of all services with a status of `Up` or `Exit 0` (for the `airflow-init` service, which only runs once).

### 3. Accessing the Services

After the services are up and running, you can access them in your browser:

-   **Airflow Web UI**: Navigate to `http://localhost:8080`
    -   **Username**: `airflow`
    -   **Password**: `airflow`
-   **API (FastAPI)**: Navigate to `http://localhost:8000/docs` to view the interactive API documentation.
-   **Streamlit Dashboard**: Navigate to `http://localhost:8501`

### 4. Running the Data Pipelines (DAGs)

The core of this project is the data pipelines orchestrated by Airflow. The main DAGs are already available in the Airflow UI.

To start the data pipeline:

1.  Open the **Airflow Web UI** at `http://localhost:8080`.
2.  You will see a list of available DAGs, such as `earthquake_master_dag`.
3.  To enable the DAG, click the toggle switch to the left of the DAG name until it turns blue (active).
4.  To trigger the DAG manually, click on the DAG name to go to the detail view, then click the "Play" button (▶️) in the top-right corner and select "*Trigger DAG*".
5.  You can monitor the DAG's execution progress in the *Graph*, *Grid*, or *Gantt* views.

The main DAG, `earthquake_master_dag`, orchestrates the entire data processing workflow. Here's a closer look at its key stages:

-   **Data Ingestion**: It starts by ingesting historical earthquake data directly from the **USGS (United States Geological Survey)** public API. This task is handled by an **Apache Spark** job to efficiently process the initial dataset.
-   **Spatial Processing & Clustering**: The pipeline performs spatial processing and uses the **DBSCAN** algorithm to identify significant earthquake hotspots or clusters.
-   **Risk Zone Classification**: A key feature of this project is the classification of seismic risk zones. The pipeline trains two models, **Random Forest** and **Logistic Regression**, to evaluate their performance. Based on consistent results, the `earthquake_master_dag` selects the best-performing model—typically **Random Forest**—and uses its saved `.pkl` file to classify the risk zones in the dataset.

The results from this pipeline will be saved and made accessible through the API service and visualized on the Streamlit dashboard.

### 5. Stopping the Application

To stop all running services, execute the following command in your terminal from the project's root directory:

```bash
docker-compose down
```

This command will stop and remove the containers, but any data stored in volumes (like the Postgres data) will persist for future use.

## Key Folder Structure

-   `./airflow/dags/`: Contains all Python DAG definitions for Airflow.
-   `./airflow/operators/`: Contains custom operators used within the DAGs.
-   `./api/`: Source code for the FastAPI service.
-   `./streamlit/`: Source code for the Streamlit dashboard application.
-   `./data/`: Default output directory for data generated by the Airflow pipelines.
-   `docker-compose.yml`: The main configuration file for orchestrating all services.
