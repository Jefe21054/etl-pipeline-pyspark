import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from etl_pipeline import (
    extract_movies_to_df,
    extract_users_to_df,
    transform_avg_ratings,
    load_df_to_db)


# Define ETL Function
def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(transformed_df)


# Define the arguments for the DAG
default_args = {
    'owner': 'jefe21054',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'email': ['jefe21054@example.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

# Instantiate the DAG
dag = DAG(dag_id='etl_pipeline',
          default_args=default_args,
          schedule_interval='0 0 * * *')

# Define the ETL Task
etl_task = PythonOperator(task_id='etl_task',
                          python_callable=etl,
                          dag=dag)

etl()
