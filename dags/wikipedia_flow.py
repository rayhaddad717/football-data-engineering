from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator

# Add the parent directory to the path so that we can import the wikipedia_pipeline module
# Too add the football data engineering folder to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import (
    extract_wikipedia_data,
    transform_wikipedia_data,
    write_wikipedia_data,
)

dag = DAG(
    dag_id="wikipedia_flow",
    default_args={
        "owner": "Ray Haddad",
        "start_date": datetime(2024, 8, 1),
    },
    schedule_interval=None,
    catchup=False,
)

# Extraction
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    },
    dag=dag,
)


# Transform

transform_wikipedia_data = PythonOperator(
    task_id="transform_wikipedia_data",
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag,
)

# Write

write_wikipedia_data = PythonOperator(
    task_id="write_wikipedia_data",
    provide_context=True,
    python_callable=write_wikipedia_data,
    dag=dag,
)
# def main():
#     print("This script is being run directly")


# # determine if the script is being run directly or being imported
# if __name__ == "__main__":
#     main()
