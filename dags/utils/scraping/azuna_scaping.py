import requests
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

lock = threading.Lock()  # Pour protéger l'accès concurrent à results_all

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constantes de l'API
APP_ID = "04988752"
APP_KEY = "667120b0097c033ac324fc37f6f64252"
BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# Pays ciblés et mots-clés
COUNTRIES = ['fr', 'de', 'nl', 'it', 'es', 'pl', 'be', 'at', 'ch','se']

JOBS = [
    'data engineer', 'data scientist', 'machine learning', 'python', 'developer',
    'software engineer', 'cloud', 'devops', 'ai', 'backend', 'frontend'
]

def safe_request(url, params, max_retries=3, backoff=3):
    for i in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Tentative {i+1}/{max_retries} échouée: {e}")
            time.sleep(backoff * (i + 1))  # exponential backoff
    raise Exception(f"Échec de la requête après {max_retries} tentatives.")


def fetch_one_page(country, job, page, app_id, app_key):
    logger.info(f"🔎 {country.upper()} | {job} | page {page}")
    url = f"{BASE_URL}/{country}/search/{page}"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": job,
        "results_per_page": 50,
        "content-type": "application/json"
    }

    try:
        data = safe_request(url, params)

        if "results" not in data:
            logger.warning(f"⚠️ Aucune donnée pour {country}-{job}-page{page}")
            return []

        offers = []
        for job_offer in data["results"]:
            min_salary = job_offer.get("salary_min")
            max_salary = job_offer.get("salary_max")

            if min_salary and max_salary:
                offers.append({
                    "poste": job_offer.get("title"),
                    "entreprise": job_offer.get("company", {}).get("display_name"),
                    "lieu": job_offer.get("location", {}).get("display_name"),
                    "salary_min": int(min_salary),
                    "salary_max": int(max_salary),
                    "currency": job_offer.get("salary_currency"),
                    "description": job_offer.get("description"),
                    "source": "adzuna_api",
                    "pays": country
                })

        return offers

    except Exception as e:
        logger.error(f"Erreur API pour {country}-{job}-page{page} : {e}")
        return []
    
def fetch_adzuna_offers(app_id=APP_ID, app_key=APP_KEY, countries=COUNTRIES, jobs=JOBS, max_pages=4, max_workers=20):
    results_all = []

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for country in countries:
            for job in jobs:
                for page in range(1, max_pages + 1):
                    tasks.append(executor.submit(fetch_one_page, country, job, page, app_id, app_key))

        for future in as_completed(tasks):
            try:
                result = future.result()
                with lock:
                    results_all.extend(result)
            except Exception as e:
                logger.error(f"Erreur dans une tâche : {e}")

    return results_all


def save_offers_to_csv(offers: list, output_path: str):
    """
    Sauvegarde les offres dans un fichier CSV.
    """
    if not offers:
        logger.warning("Aucune offre à sauvegarder.")
        return

    df = pd.DataFrame(offers)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"{len(df)} offres avec salaires sauvegardées dans {output_path}")

    return df


def preview_offers(df: pd.DataFrame, n=10):
    """
    Affiche un aperçu des offres (colonnes principales).
    """
    logger.info("Aperçu des premières lignes :")
    print(df[["poste", "entreprise", "lieu", "salary_min", "salary_max", "pays"]].head(n))



