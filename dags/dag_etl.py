from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import pandas as pd
import os
import psycopg2
import psycopg2.extras as extras

def extract_data() {
    df = 
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1), 
}

with DAG(
    'COVID-19',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    
) as dag:
    
    extract = BashOperator(
        task_id='Extract',
        bash_command='curl --keepalive-time 6000 -o ${AIRFLOW_HOME}/data/valeurs_foncieres.txt https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20231010-093059/valeursfoncieres-2022.txt',
    )
    
    # transform = PythonOperator(
    #     task_id='Transform',
    #     python_callable=data_transform,
    #     dag=dag,
    # )
    
    # load = PythonOperator(
    #     task_id='Load',
    #     python_callable=load_values,
    #     dag=dag,
    # )

    # create_table = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id='postgres_connexion',
    #     sql='sql/create_table.sql',
    # ) 
    
    # transform_and_load = PythonOperator(
    #     task_id='transform_and_load',
    #     python_callable=load_values,

    extract >> transform >> load