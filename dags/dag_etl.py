from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
from airflow.configuration import conf
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

def data_transform_and_load():
    df = extract_donnees_urgences_SOS_medecins()
    
    # Liste des colonnes à supprimer    
    columns_to_drop = ["nbre_acte_corona", "nbre_acte_tot", "nbre_acte_corona_h", "nbre_acte_corona_f", "nbre_acte_tot_h", "nbre_acte_tot_f"]
    
    # Suppression des colonnes
    df = df.drop(columns=columns_to_drop, axis=1)

    columns_to_fill = ["nbre_pass_corona", "nbre_pass_tot", "nbre_hospit_corona"]
    df[columns_to_fill] = df[columns_to_fill].fillna('0')
    
    # df.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/test.csv"), sep=";", index=False)
    
    ### Insertion des données dans la table "Departement"
    
    df_departements_region = extract_departements_region()
    df_departements_region = df_departements_region.rename(columns={"num_dep": "num_departement", "dep_name": "nom_departement", "region_name": "nom_region"})

    df_departements_region['num_departement'] = df_departements_region['num_departement'].astype(str)
    df_departements_region['nom_departement'] = df_departements_region['nom_departement'].astype(str)
    df_departements_region['nom_region'] = df_departements_region['nom_region'].astype(str)

    pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for _, row in df_departements_region.iterrows():
        cursor.execute("INSERT INTO departement (num_departement, nom_departement, nom_region) VALUES (%s, %s, %s) ON CONFLICT (num_departement) DO NOTHING",
                       (row['num_departement'], row['nom_departement'], row['nom_region']))

    pg_conn.commit()
    cursor.close()
    
    ### Insertion des données dans la table "Passage"
    
    df_passage = df[["nbre_pass_corona", "nbre_pass_tot", "nbre_pass_corona_h", "nbre_pass_corona_f", "nbre_pass_tot_h", "nbre_pass_tot_f"]]

    df_passage['nbre_pass_corona'] = df_passage['nbre_pass_corona'].fillna(0).astype(int)
    df_passage['nbre_pass_tot'] = df_passage['nbre_pass_tot'].fillna(0).astype(int)
    df_passage['nbre_pass_corona_h'] = df_passage['nbre_pass_corona_h'].fillna(0).astype(int)
    df_passage['nbre_pass_corona_f'] = df_passage['nbre_pass_corona_f'].fillna(0).astype(int)
    df_passage['nbre_pass_tot_h'] = df_passage['nbre_pass_tot_h'].fillna(0).astype(int)
    df_passage['nbre_pass_tot_f'] = df_passage['nbre_pass_tot_f'].fillna(0).astype(int)

    pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for _, row in df_passage.iterrows():
        cursor.execute("INSERT INTO passage (nombre_passage_corona, nombre_passage_total, nombre_passage_corona_h, nombre_passage_corona_f, nombre_passage_total_h, nombre_passage_total_f) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id_passage) DO NOTHING",
                       (row['nbre_pass_corona'], row['nbre_pass_tot'], row['nbre_pass_corona_h'], row['nbre_pass_corona_f'], row['nbre_pass_tot_h'], row['nbre_pass_tot_f']))

    pg_conn.commit()
    cursor.close()

    return "Data inserted successfully"

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
    
    dag_create_table = PostgresOperator(
        task_id='Create_table',
        postgres_conn_id='postgres_connexion',  # Remplacez par votre ID de connexion PostgreSQL
        sql='sql/create_table.sql',  # Chemin vers votre script SQL
    )
    
    dag_data_transform_and_load = PythonOperator(
        task_id="Data_transform_and_load",
        python_callable = data_transform_and_load,
        dag=dag,
    )

    # dag_insert_data = PythonOperator(
    #     task_id='insert_data',
    #     python_callable=insert_data_into_db,
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

    dag_extract_donnees_urgences_SOS_medecins >> dag_extract_tranche_age_donnee_urgences >> dag_extract_departements_region >> dag_data_transform_and_load