from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from earthquakes import extract_earthquakes, transform_earthquakes
from tsunamis import extract_tsunamis, transform_tsunamis
from volcanoes import extract_volcanoes, transform_volcanoes
from load_mongo import load_to_mongo

with DAG(
    "chile_natural_hazards_pipeline",
    description="ETL pipeline para terremotos, tsunamis y volcanes en Chile (2010-2020)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Cambia a tu necesidad, ejemplo '0 0 * * *' para diario
    catchup=False
) as dag:

    # Terremotos
    extract_eq = PythonOperator(task_id="extract_earthquakes", python_callable=extract_earthquakes)
    transform_eq = PythonOperator(task_id="transform_earthquakes", python_callable=transform_earthquakes)

    # Tsunamis
    extract_ts = PythonOperator(task_id="extract_tsunamis", python_callable=extract_tsunamis)
    transform_ts = PythonOperator(task_id="transform_tsunamis", python_callable=transform_tsunamis)

    # Volcanes
    extract_vc = PythonOperator(task_id="extract_volcanoes", python_callable=extract_volcanoes)
    transform_vc = PythonOperator(task_id="transform_volcanoes", python_callable=transform_volcanoes)

    # Carga final a MongoDB
    load = PythonOperator(task_id="load_to_mongo", python_callable=load_to_mongo)

    # Definir dependencias
    extract_eq >> transform_eq
    extract_ts >> transform_ts
    extract_vc >> transform_vc

    [transform_eq, transform_ts, transform_vc] >> load
