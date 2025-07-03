from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pytrends.request import TrendReq
import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

keywords = [
    'Python', 'Java', 'JavaScript', 'Go', 'Rust', 'C++',
    'C#', 'Kotlin', 'React', 'Vue.js', 'Angular'
]

countries = [
    'FR', 'DE', 'NL', 'ES', 'PL', 'IT', 'BE', 'SE', 'FI',
]

# ✅ Utilise un format sûr pour test
timeframes = [
    "today 3-m"
]

output_file = "/home/fboubekri/data_challenge/output/google_trend.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

lock = threading.Lock()

def prepare_google_trends_tasks(**context):
    # Charger données déjà extraites
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            done = existing_df['timeframe'].unique().tolist()
        except Exception as e:
            print(f"⚠️ Erreur lecture fichier existant : {e}")
            done = []
    else:
        done = []

    tasks = []
    for tf in timeframes:
        if tf in done:
            print(f"✅ Période déjà traitée : {tf}")
            continue
        for kw in keywords:
            for country in countries:
                tasks.append((kw, country, tf))

    print(f"🗂️ {len(tasks)} tâches Google Trends préparées.")
    context['ti'].xcom_push(key='tasks', value=tasks)

def fetch_trends(kw, country, tf):
    try:
        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload([kw], timeframe=tf, geo=country)
        df = pytrends.interest_over_time()
        print(f"↪️ {kw}-{country}-{tf} | rows: {len(df)}")

        if not df.empty:
            df['keyword'] = kw
            df['country'] = country
            df['timeframe'] = tf
            df.reset_index(inplace=True)
            df = df[['date', 'keyword', 'country', 'timeframe', kw]]
            df.rename(columns={kw: 'popularity'}, inplace=True)
            print(f"✅ Done {kw} - {country} - {tf}")
            return df
        else:
            print(f"⚠️ Empty data {kw} - {country} - {tf}")
            return None
    except Exception as e:
        print(f"❌ Erreur dans fetch_trends pour {kw}-{country}-{tf} : {e}")
        return None

def scrape_google_trends_parallel(**context):
    tasks = context['ti'].xcom_pull(key='tasks', task_ids='prepare_google_trends_tasks')
    print(f"📦 Nombre de tâches récupérées depuis XCom : {len(tasks)}")
    
    all_data = []
    max_workers = 5

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_trends, kw, country, tf) for kw, country, tf in tasks]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    with lock:
                        all_data.append(result)
                time.sleep(1)
            except Exception as e:
                print(f"❌ Erreur thread: {e}")

    print(f"📊 Nombre total de DataFrames collectés : {len(all_data)}")
    print("⏸️ Pause 30 secondes avant la fin...")
    time.sleep(30)

    if len(all_data) > 0:
        final_df = pd.concat(all_data, ignore_index=True)
        for tf in final_df['timeframe'].unique():
            df_tf = final_df[final_df['timeframe'] == tf]
            save_results_to_csv(df_tf, tf)
    else:
        print("❌ Aucune donnée récupérée.")

def save_results_to_csv(df: pd.DataFrame, tf: str):
    output_dir = "/home/fboubekri/data_challenge/output"
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, "google_trend.csv")  # 🔒 Nom de fichier fixe

    try:
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates()

        df.to_csv(file_path, index=False)
        print(f"✅ Données sauvegardées dans {file_path}")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde dans {file_path} : {e}")
