# Spotify 2025 Top Tracks: End-to-End ETL & Analytics Pipeline

## Project Overview
This repository contains an end-to-end data pipeline that automates data ingestion, cleaning, data quality validation, and visualization of the most streamed tracks on Spotify in 2025. 

## Data Architecture & Pipeline Stages
The entire pipeline is fully orchestrated by **Apache Airflow** using **TaskFlow API** and runs within **Docker** environment.

`Source CSV (Kaggle) ➔ Apache Airflow ➔ Data Quality Check ➔ PostgreSQL DWH ➔ Apache Superset`

1. **Extract**: Extracts raw data from the Spotify 2025 CSV dataset.
2. **Transform**: Validates data types, cleans strings, handles potential missing values, and calculates custom metric `is_mega_hit` based on streaming thresholds.
3. **Data Quality Check**: A dedicated staging task verifies row counts and prevents zero-rows or corrupted data frames from advancing further down the pipeline, raising explicit `ValueError` blocks on failure.
4. **Load (DWH)**: Executes`UPSERT` operations (`INSERT ... ON CONFLICT DO UPDATE`) into a PostgreSQL database, completely eliminating data duplication and ensuring strict data warehouse consistency.
5. **Analyze & Visualize**: Performs aggregation queries (`GROUP BY`, `SUM`) directly inside the database engine to isolate Top 3 artists and surfaces insights on live **Apache Superset** dashboards.

## Tech Stack
* **Orchestration:** Apache Airflow (TaskFlow API)
* **Database & DWH:** PostgreSQL
* **BI & Visualization:** Apache Superset
* **Containerization:** Docker & Docker Compose
* **Development Environment:** WSL (Ubuntu), VS Code
* **Core Languages:** Python 3.x (Clean OOP/Functional concepts), SQL (Analytical window functions and aggregates)

## Fault Tolerance & Production Readiness
* **Automated Retries**: Configured globally via `default_args` using a 3-tier retry policy with `timedelta` intervals to handle potential database drops.
* **Idempotency**: The pipeline is fully idempotent due to strict unique constraints on the destination table (`UNIQUE(track_name, artist_name)`). Running the DAG multiple times updates existing metrics rather than breaking or duplicating the dataset.

## How to Run Locally

### 1. Prerequisites
Make sure you have **Docker Desktop** installed and running on your host machine (WSL Ubuntu environment is recommended).

### 2. Spin up the Infrastructure
Clone this repository to your local directory and run:
```
bash
docker-compose up -d
```
This command will pull and launch all required containers: Apache Airflow (Scheduler, Webserver), PostgreSQL, and Apache Superset.

#### 3. Trigger the Pipeline
1. Put your most_streamed_spotify_2025.csv file inside the dags/data/ directory.
2. Open your browser and navigate to the Airflow UI at http://localhost:8080 (Default credentials: airflow / airflow).
3. Turn on the spotify_pipeline DAG and click Trigger DAG.

### 4. Explore the Analytical Dashboard
1. Open the Apache Superset UI at http://localhost:8088.
2. Navigate to Dashboards and explore the pre-built charts.
3. *(Optional)* If you are setting it up from scratch, you can instantly import the ready-made dashboard config file located in the /superset/dashboards/ directory of this repo.
