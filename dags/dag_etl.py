from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import pandas as pd
import os
import psycopg2
import psycopg2.extras as extras

def extract_donnees_urgences_SOS_medecins():
    df_donnees_urgences_SOS_medecins = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv"), 
                    sep=";", 
                    dtype="unicode")    

    return df_donnees_urgences_SOS_medecins

def extract_tranche_age_donnee_urgences():
    df_tranche_age_donnee_urgences = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv"), 
                    sep=";", 
                    dtype="unicode")    

    return df_tranche_age_donnee_urgences

def extract_departements_region():
    df_departements_region = pd.read_json(os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json"),
                                          dtype="unicode")   

    return df_departements_region

def transform_data():
    df = extract_donnees_urgences_SOS_medecins()
    
    # Liste des colonnes à supprimer    
    columns_to_drop = ["nbre_acte_corona", "nbre_acte_tot", "nbre_acte_corona_h", "nbre_acte_corona_f", "nbre_acte_tot_h", "nbre_acte_tot_f"]
    
    # Suppression des colonnes
    df = df.drop(columns=columns_to_drop, axis=1)

    columns_to_fill = ["nbre_pass_corona", "nbre_pass_tot", "nbre_hospit_corona"]
    df[columns_to_fill] = df[columns_to_fill].fillna('0')
    
    df.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/test.csv"), sep=";", index=False)
    
    return df

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
    
    dag_extract_donnees_urgences_SOS_medecins = PythonOperator(
        task_id="Extract_donnees_SOS_medecins",
        python_callable = extract_donnees_urgences_SOS_medecins,
        dag=dag,
    )
    
    dag_extract_tranche_age_donnee_urgences = PythonOperator(
        task_id="Extract_des_tranches_age_des_données_urgences",
        python_callable = extract_tranche_age_donnee_urgences,
        dag=dag,
    )
    
    dag_extract_departements_region = PythonOperator(
        task_id="Extract_departements_region",
        python_callable = extract_departements_region,
        dag=dag,
    )
    
    dag_transform_data = PythonOperator(
        task_id="Transform_data",
        python_callable = transform_data,
        dag=dag,
    )
    
    # create_table = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id='postgres_connexion',
    #     sql='sql/create_table.sql',
    # ) 
    
    # transform_and_load = PythonOperator(
    #     task_id='transform_and_load',
    #     python_callable=load_values,

    dag_extract_donnees_urgences_SOS_medecins >> dag_extract_tranche_age_donnee_urgences >> dag_extract_departements_region >> dag_transform_data