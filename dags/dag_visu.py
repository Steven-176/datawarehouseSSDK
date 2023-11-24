from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
}

def execute_query(requete):
    pg_hook = PostgresHook(postgres_conn_id='postgres_connexion')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(requete)
    result = cursor.fetchall()
    for i in result:
        print(i)
    cursor.close()
    pg_conn.close()

def analyse_suspicion_covid_mois_region():
    requete = """
        SELECT 
            EXTRACT(MONTH FROM date_de_passage) AS month,
            EXTRACT(YEAR FROM date_de_passage) AS year,
            nom_region,
            SUM(nombre_passage_corona) / NULLIF(SUM(nombre_passage_total), 0) * 100 AS pourcentage_passage_corona
        FROM passage
        JOIN urgence_covid ON passage.id_passage = urgence_covid.id_passage
        JOIN departement ON urgence_covid.num_departement = departement.num_departement
        GROUP BY month, year, nom_region;
    """
    execute_query(requete)

def analyse_suspicion_covid_tranche_age():
    requete = """
        SELECT 
            EXTRACT(YEAR FROM date_de_passage) AS year,
            tranche_age,
            SUM(nombre_passage_corona) / NULLIF(SUM(nombre_passage_total), 0) * 100 AS pourcentage_passage_corona
        FROM passage
        JOIN urgence_covid ON passage.id_passage = urgence_covid.id_passage
        JOIN age ON urgence_covid.id_age = age.id_age
        WHERE EXTRACT(YEAR FROM date_de_passage) = 2022
        GROUP BY year, tranche_age;
    """
    execute_query(requete)

def analyse_suspicion_covid_personnes_vieux():
    requete = """
        SELECT 
            EXTRACT(YEAR FROM date_de_passage) AS year,
            SUM(nombre_passage_corona) / NULLIF(SUM(nombre_passage_total), 0) * 100 AS pourcentage_passage_corona
        FROM passage
        JOIN urgence_covid ON passage.id_passage = urgence_covid.id_passage
        JOIN age ON urgence_covid.id_age = age.id_age
        WHERE EXTRACT(YEAR FROM date_de_passage) = 2023 AND tranche_age = '65-74 ans' OR tranche_age = '75 ans et plus'
        GROUP BY year;
    """
    execute_query(requete)

def analyse_suspicion_covid_femmes():
    requete = """
        SELECT 
            EXTRACT(YEAR FROM date_de_passage) AS year,
            num_departement,
            nom_departement,
            SUM(nombre_passage_corona_f) / NULLIF(SUM(nombre_passage_total_f), 0) * 100 AS pourcentage_passage_corona_f
        FROM passage
        JOIN urgence_covid ON passage.id_passage = urgence_covid.id_passage
        JOIN departement ON urgence_covid.num_departement = departement.num_departement
        GROUP BY year, num_departement, nom_departement;
    """
    execute_query(requete)

def analyse_suspicion_covid_hommes():
    requete = """
        SELECT 
            EXTRACT(YEAR FROM date_de_passage) AS year,
            num_departement,
            nom_departement,
            SUM(nombre_passage_corona_h) / NULLIF(SUM(nombre_passage_total_h), 0) * 100 AS pourcentage_passage_corona_h
        FROM passage
        JOIN urgence_covid ON passage.id_passage = urgence_covid.id_passage
        JOIN departement ON urgence_covid.num_departement = departement.num_departement
        GROUP BY year, num_departement, nom_departement;
    """
    execute_query(requete)

def analyse_suspicion_covid_homme_femme():
    requete = """
        SELECT 
            date_de_passage,
            nom_region,
            SUM(nombre_hospitalisation_corona_h) / NULLIF(SUM(nombre_hospitalisation_corona_f), 0) AS rapport_hospitalisations_h_f
        FROM hospitalisation
        JOIN urgence_covid ON hospitalisation.id_hospitalisation = urgence_covid.id_hospitalisation
        JOIN departement ON urgence_covid.num_departement = departement.num_departement
        GROUP BY date_de_passage, nom_region;
    """
    execute_query(requete)

with DAG('analyse_covid19', default_args=default_args, schedule_interval=None) as dag:

    dag_analyse_covid_mois = PythonOperator(
        task_id='analyse_suspicion_covid_mois_region',
        python_callable=analyse_suspicion_covid_mois_region,
    )

    dag_analyse_covid_age = PythonOperator(
        task_id='analyse_suspicion_covid_tranche_age',
        python_callable=analyse_suspicion_covid_tranche_age,
    )

    dag_analyse_covid_vieux = PythonOperator(
        task_id='analyse_suspicion_covid_personnes_vieux',
        python_callable=analyse_suspicion_covid_personnes_vieux,
    )

    dag_analyse_covid_femmes = PythonOperator(
        task_id='analyse_suspicion_covid_femmes',
        python_callable=analyse_suspicion_covid_femmes,
    )

    dag_analyse_covid_hommes = PythonOperator(
        task_id='analyse_suspicion_covid_hommes',
        python_callable=analyse_suspicion_covid_hommes,
    )

    dag_analyse_covid_homme_femme = PythonOperator(
        task_id='analyse_suspicion_covid_homme_femme',
        python_callable=analyse_suspicion_covid_homme_femme,
    )

    dag_analyse_covid_mois >> dag_analyse_covid_age >> dag_analyse_covid_vieux >> dag_analyse_covid_femmes >> dag_analyse_covid_hommes >> dag_analyse_covid_homme_femme