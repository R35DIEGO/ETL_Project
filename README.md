Project: Airflow-ETL Real-World Data Pipeline and Dashboard

Massive Data Management - 5B. Diego Loria

Project Overview

This project implements an end-to-end ETL pipeline using Apache Airflow to extract, transform, and load data about natural hazards in Chile — including earthquakes, tsunamis, and volcanic eruptions — into MongoDB. A Streamlit dashboard is also included for visualizing the processed data. PostgreSQL is used to handle Airflow's metadata, while MongoDB stores the datasets gathered from the public APIs.

APIs Used

Earthquake API: USGS Earthquake APIExtracts recent and historical earthquake events based on date, region, and magnitude.

Tsunami API: NOAA Hazards API (Tsunamis)Retrieves tsunami event information, including date, fatalities, wave heights, and locations.

Volcano API: NOAA Volcano Feature Layer APIProvides detailed volcanic activity in Chile, including names, VEI (Volcanic Explosivity Index), and geolocation.

MongoDB vs PostgreSQL Clarification

MongoDB: Serves as the primary data warehouse for natural hazard events. It stores raw and transformed data with flexibility for various data structures (earthquakes, tsunamis, volcanoes).

PostgreSQL: Used internally by Apache Airflow to manage DAGs, task scheduling, XComs, and logging metadata.

How to Launch Each Service with Docker-Compose

Ensure Docker and Docker Compose are installed on your system.

In the project root directory, execute:

docker-compose up -d

Default login credentials user: admin password: admin

This launches:

PostgreSQL (Airflow backend)

MongoDB (data warehouse)

Apache Airflow (Webserver and Scheduler)

Streamlit (interactive dashboard)

How to Trigger the DAG and Check Logs

Open Airflow Web UI:

http://localhost:8080

Log in with default credentials.

Find the DAG named chile_natural_hazards_etl

Turn it ON (if paused).

Click ▶ to trigger manually.

Review logs and progress for each task directly in the interface.

How to Open the Streamlit Dashboard

Navigate to:

http://localhost:8501

Use filters to explore:

Earthquakes: Magnitude, region, coordinates.

Tsunamis: Fatalities, location, runup height.

Volcanoes: Name, VEI, eruption year, geolocation.

All data is loaded from MongoDB for dynamic visualization.

Explanation of XCom Usage

XComs (Cross-communication messages) in Airflow are used to share metadata between tasks. In this project, XComs help:

Track the number of records extracted/transformed/loaded.

Share dates or location filters between extract and transform tasks.

Enable condition-based branching if needed (e.g., skip if no new records).

These features help monitor the pipeline performance and ensure correct task dependencies.
