from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import csv
import os
import pandas as pd
from functools import partial
from utils.scraping.glassdor_scraping import *  # cette fonction doit prendre URL et chemin de sortie
from utils.scraping.azuna_scaping import *
from utils.scraping.github_trending_scraping import *
from utils.mongo import *
from utils.cleaning import *

from utils.scraping.meteojob_scraping import *
from utils.scraping.google_trend import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


urls = [
    "https://www.glassdoor.fr/Emploi/france-data-emplois-SRCH_IL.0,6_IN86_KO7,11.htm",
    "https://www.glassdoor.fr/Emploi/france-developer-emplois-SRCH_IL.0,6_IN86_KO7,13.htm",
    "https://www.glassdoor.fr/Emploi/france-tech-emplois-SRCH_IL.0,6_IN86_KO7,24.htm",
    "https://www.glassdoor.fr/Emploi/allemagne-data-emplois-SRCH_IL.0,6_IN86_KO7,11.htm",
    "https://www.glassdoor.fr/Emploi/allemagne-developer-emplois-SRCH_IL.0,6_IN86_KO7,13.htm",
    "https://www.glassdoor.fr/Emploi/allemagne-tech-emplois-SRCH_IL.0,6_IN86_KO7,24.htm",
    "https://www.glassdoor.fr/Emploi/espagne-data-emplois-SRCH_IL.0,6_IN86_KO7,11.htm",
    "https://www.glassdoor.fr/Emploi/espage-developer-emplois-SRCH_IL.0,6_IN86_KO7,13.htm",
    "https://www.glassdoor.fr/Emploi/espagne-tech-emplois-SRCH_IL.0,6_IN86_KO7,24.htm",
    "https://www.glassdoor.fr/Emploi/belgique-data-emplois-SRCH_IL.0,6_IN86_KO7,11.htm",
    "https://www.glassdoor.fr/Emploi/belgique-developer-emplois-SRCH_IL.0,6_IN86_KO7,13.htm",
    "https://www.glassdoor.fr/Emploi/belgique-tech-emplois-SRCH_IL.0,6_IN86_KO7,24.htm",
    
]

# Dossier temporaire pour stocker les fichiers CSV intermédiaires
OUTPUT_DIR = "/home/fboubekri/data_challenge/output"
FINAL_OUTPUT = "/home/fboubekri/data_challenge/output/all_jobs.csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configuration DAG
default_args = {
    'owner': 'fboubekri',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 7, 1),
}

def run_adzuna_scraping():
    offers = fetch_adzuna_offers()
    save_offers_to_csv(offers, "/home/fboubekri/data_challenge/output/adzuna_multi_country.csv")

with DAG(
    dag_id='multi_url_scraping_glassdoor',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={},  # ✅ Ajouté pour éviter l'erreur
    tags=['scraping', 'selenium'],
) as dag:

    task_scrape_adzuna = PythonOperator(
        task_id='scrape_adzuna_jobs',
        python_callable=run_adzuna_scraping,
    )

    task_scrape_parallel = PythonOperator(
        task_id='scrape_all_in_parallel',
        python_callable=lambda: parallel_scraping(urls, OUTPUT_DIR),
    )

    task_concat = PythonOperator(
        task_id='concat_all_csvs',
        python_callable=lambda: concat_csv_files(OUTPUT_DIR, FINAL_OUTPUT),
    )

    # task_concat = PythonOperator(
    #     task_id='concat_and_save_jobs_to_mongodb',
    #     python_callable=lambda: concat_and_save_jobs_csv_to_mongodb(OUTPUT_DIR, "adzuna_jobs"),
    # )

    task_insert_glassdor_to_mongo = PythonOperator(
    task_id='insert_glassdor_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/all_jobs.csv", 
            "glassdoor"
        ),
)


    task_scrape_github_trending = PythonOperator(
        task_id='scrape_github_trending',
        python_callable=run_github_scraping,
    )

    task_insert_adzuna_to_mongo = PythonOperator(
    task_id='insert_adzuna_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/adzuna_multi_country.csv", 
            "adzuna_jobs"
        ),
)

    task_insert_github_to_mongo = PythonOperator(
        task_id='insert_github_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/github_trending.csv", 
            "github_trending"
        ),
)
    task_scrape_meteojob = PythonOperator(
        task_id='scrape_meteojob_jobs',
        python_callable=run_meteojob_scraping,
    )

    task_insert_meteojob_to_mongo = PythonOperator(
        task_id='insert_meteojob_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/meteojob.csv", 
            "meteojob"
        ),
)
    task_prepare_google_trends_tasks = PythonOperator(
        task_id='prepare_google_trends_tasks',
        python_callable=prepare_google_trends_tasks,
        provide_context=True,
    )

    task_scrape_google_trends_parallel = PythonOperator(
        task_id='scrape_google_trends_parallel',
        python_callable=scrape_google_trends_parallel,
        provide_context=True,
    )

    task_insert_google_trend_to_mongo = PythonOperator(
        task_id='insert_google_trend_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/google_trend.csv", 
            "google_trend"
        ),
    )

    task_insert_stepstone_jobs_to_mongo = PythonOperator(
        task_id='insert_stepsstone_jobs_to_mongodb',
        python_callable=lambda: insert_csv_to_mongodb(
            "/home/fboubekri/data_challenge/output/stepstone_jobs.csv", 
            "stepstone_jobs"
        ),
    )


    etl_task = PythonOperator(
        task_id="transform_and_load_job_data",
        python_callable=etl_job_data_to_postgres
    )



    task_scrape_adzuna >>  task_insert_adzuna_to_mongo

    task_prepare_google_trends_tasks >> task_scrape_google_trends_parallel >> task_insert_google_trend_to_mongo

    task_scrape_parallel >> task_concat >> task_insert_glassdor_to_mongo

    task_scrape_github_trending  >> task_insert_github_to_mongo

    task_scrape_meteojob >> task_insert_meteojob_to_mongo

    task_insert_stepstone_jobs_to_mongo

    etl_task

