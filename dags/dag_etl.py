from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os

# Fonction extraction données urgences médecins
def extract_donnees_urgences_SOS_medecins():
    df_donnees_urgences_SOS_medecins = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv"), 
                    sep=";", 
                    dtype="unicode")    

    return df_donnees_urgences_SOS_medecins

# Fonction extraction données tranche d'age
def extract_tranche_age_donnee_urgences():
    df_tranche_age_donnee_urgences = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv"), 
                    sep=";", 
                    dtype="unicode")    

    return df_tranche_age_donnee_urgences

# Fonction extraction données departements/regions
def extract_departements_region():
    df_departements_region = pd.read_json(os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json"),
                                          dtype="unicode")   

    return df_departements_region

# Fonction de transformation et de chargement des données dans la BDD
def data_transform_and_load():

    # Récupération des données d'urgences dans un Dataframe
    df = extract_donnees_urgences_SOS_medecins()

    # Liste des colonnes à supprimer
    columns_to_drop = ["nbre_acte_corona", "nbre_acte_tot", "nbre_acte_corona_h", "nbre_acte_corona_f", "nbre_acte_tot_h", "nbre_acte_tot_f"]

    # Suppression des colonnes
    df = df.drop(columns=columns_to_drop, axis=1)
    columns_to_fill = ["nbre_pass_corona", "nbre_pass_tot", "nbre_hospit_corona"]
    df[columns_to_fill] = df[columns_to_fill].fillna('0')

    # Typage de la colonne date de passage en format Date
    df['date_de_passage'] = pd.to_datetime(df['date_de_passage'])

    ### Insertion des données dans la table "Departement"
    df_departements_region = extract_departements_region()
    df_departements_region = df_departements_region.rename(columns={"num_dep": "num_departement", "dep_name": "nom_departement", "region_name": "nom_region"})

    df_departements_region['num_departement'] = df_departements_region['num_departement'].astype(str)
    df_departements_region['nom_departement'] = df_departements_region['nom_departement'].astype(str)
    df_departements_region['nom_region'] = df_departements_region['nom_region'].astype(str)
    

    ### Typage des données dans la table "Passage"
    df['nbre_pass_corona'] = df['nbre_pass_corona'].fillna(0).astype(int)
    df['nbre_pass_tot'] = df['nbre_pass_tot'].fillna(0).astype(int)
    df['nbre_pass_corona_h'] = df['nbre_pass_corona_h'].fillna(0).astype(int)
    df['nbre_pass_corona_f'] = df['nbre_pass_corona_f'].fillna(0).astype(int)
    df['nbre_pass_tot_h'] = df['nbre_pass_tot_h'].fillna(0).astype(int)
    df['nbre_pass_tot_f'] = df['nbre_pass_tot_f'].fillna(0).astype(int)

    
    ### Typage des données dans la table "Hospitalisation"
    df['nbre_hospit_corona'] = df['nbre_hospit_corona'].fillna(0).astype(int)
    df['nbre_hospit_corona_h'] = df['nbre_hospit_corona_h'].fillna(0).astype(int)
    df['nbre_hospit_corona_f'] = df['nbre_hospit_corona_f'].fillna(0).astype(int)


    # Ouverture de la connexion avec la BDD
    pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Table Departement
    for _, row in df_departements_region.iterrows():
        cursor.execute("INSERT INTO departement (num_departement, nom_departement, nom_region) VALUES (%s, %s, %s) ON CONFLICT (num_departement) DO NOTHING",
                       (row['num_departement'], row['nom_departement'], row['nom_region']))

    for _, row in df.iterrows():

        # Table Passage
        cursor.execute("INSERT INTO passage (nombre_passage_corona, nombre_passage_total, nombre_passage_corona_h, nombre_passage_corona_f, nombre_passage_total_h, nombre_passage_total_f) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id_passage    ",
                       (int(row['nbre_pass_corona']), int(row['nbre_pass_tot']), int(row['nbre_pass_corona_h']), int(row['nbre_pass_corona_f']), int(row['nbre_pass_tot_h']), int(row['nbre_pass_tot_f'])))
        id_passage = cursor.fetchone()[0]

        # Table Hospitalisation
        cursor.execute("INSERT INTO hospitalisation (nombre_hospitalisation_corona, nombre_hospitalisation_corona_h, nombre_hospitalisation_corona_f) VALUES (%s, %s, %s) RETURNING id_hospitalisation",
                       (int(row['nbre_hospit_corona']), int(row['nbre_hospit_corona_h']), int(row['nbre_hospit_corona_f'])))
        id_hospitalisation = cursor.fetchone()[0]

        # Table des faits
        cursor.execute("""
                INSERT INTO urgence_covid (date_de_passage, id_hospitalisation, id_passage, id_age, num_departement)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['date_de_passage'], id_hospitalisation, id_passage, row['sursaud_cl_age_corona'], row['dep']))

    # Lancement des requêtes
    pg_conn.commit()

    # Fermeture de la connexion
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

    dag_extract_donnees_urgences_SOS_medecins >> dag_extract_tranche_age_donnee_urgences >> dag_extract_departements_region >> dag_data_transform_and_load