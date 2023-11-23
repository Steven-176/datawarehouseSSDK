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
    
    df_departements_region = extract_departements_region()
    
    df_departements_region = df_departements_region.rename(columns={"num_dep": "num_departement", "dep_name": "nom_departement", "region_name": "nom_region"})

    df_departements_region['num_departement'] = df_departements_region['num_departement'].astype(str)
    df_departements_region['nom_departement'] = df_departements_region['nom_departement'].astype(str)
    df_departements_region['nom_region'] = df_departements_region['nom_region'].astype(str)

    pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for _, row in df_departements_region.iterrows():
        cursor.execute("INSERT INTO departement (num_departement, nom_departement, nom_region) VALUES (%s, %s, %s)",
                       (row['num_departement'], row['nom_departement'], row['nom_region']))

    pg_conn.commit()
    cursor.close()

    return "Data inserted successfully"



    # print(df_departements_region.dtypes)


    # Obtenez les informations de connexion à la base de données à partir de airflow.cfg
    # conn_id = 'postgres_connexion'  # Remplacez par votre ID de connexion PostgreSQL
    # conn_uri = conf.get('database', 'sql_alchemy_conn')
    # conn_uri = conn_uri.replace('postgres://', f'postgresql+psycopg2://')
    
    # # Créez une instance de moteur SQLAlchemy en utilisant la chaîne de connexion
    # engine = create_engine(conn_uri)
    
    # # Utilisez le moteur SQLAlchemy pour écrire les données
    # df_departements_region.to_sql('departement', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
    
    # sample_data = {"num_departement": "101", "nom_departement": "Test_Department", "nom_region": "Test_Region"}
    # sample_df = pd.DataFrame([sample_data])
    # sample_df.to_sql('departement', con=engine, if_exists='append', index=False, chunksize=1000)

    
    return df_departements_region

# def insert_data_into_db():
#     df_departements_region = extract_departements_region()
    
#     # Utilisation de PostgresHook pour se connecter à la base de données
#     pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    
#     # Obtenez la chaîne de connexion à partir de PostgresHook
#     conn_uri = pg_hook.get_uri()
    
#     # Créez une instance de moteur SQLAlchemy en utilisant la chaîne de connexion
#     engine = create_engine(conn_uri)
    
#     # Utilisez le moteur SQLAlchemy pour écrire les données
#     df_departements_region.to_sql('departement', con=engine, if_exists='replace', index=False, chunksize=1000)


    # Exemple d'insertion dans la table Departement
    # À adapter en fonction de vos besoins et des données spécifiques
    # insert_departement_query = """
    # INSERT INTO Departement (num_departement, nom_departement, nom_region) 
    # VALUES (%s, %s, %s);
    # """
    # departement_data = ...  # Données à insérer, sous forme de liste de tuples

    # # Connexion à la base de données et exécution de la requête
    # conn = pg_hook.get_conn()
    # cursor = conn.cursor()
    # cursor.executemany(insert_departement_query, departement_data)
    # conn.commit()
    # cursor.close()
    # conn.close()

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