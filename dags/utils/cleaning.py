import pandas as pd
import uuid
import numpy as np
import re
import logging
from pymongo import MongoClient
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Fonctions utilitaires ---

def normalize_salary(text):
    if pd.isna(text) or str(text).strip() == "-1":
        return None, None
    text = str(text).lower().replace(",", ".").replace("\u20ac", "").replace("k", "000")
    numbers = list(map(float, re.findall(r"\d+(?:\.\d+)?", text)))
    if "hour" in text or "heure" in text:
        numbers = [round(n * 35 * 52, 2) for n in numbers]
    elif "month" in text or "mois" in text:
        numbers = [round(n * 12, 2) for n in numbers]
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    elif len(numbers) >= 2:
        return numbers[0], numbers[1]
    return None, None

def detect_contract(job_title):
    job_title = str(job_title).lower()
    if "alternance" in job_title or "alternant" in job_title:
        return "alternance"
    elif "stage" in job_title or "stagiaire" in job_title:
        return "stage"
    elif "cdd" in job_title:
        return "cdd"
    elif "mission" in job_title:
        return "mission"
    else:
        return "cdi"

def extract_normalized_title(title):
    if pd.isna(title):
        return ""
    title_clean = re.sub(r"(-\s?\d+\s?k?-?)|(\(.*?\))|(\bH/F\b|\bF/H\b|\bH\\.F\b|\bF\\.H\b)", '', str(title), flags=re.IGNORECASE)
    title_clean = re.sub(r"\b(alternance|alternant|stage|stagiaire|cdi|cdd|mission|freelance|int\u00e9rim|apprentissage|temps partiel|temps plein)\b", '', title_clean, flags=re.IGNORECASE)
    title_clean = re.sub(r"\s+", ' ', title_clean).strip()
    return title_clean

JOBS = [
    'data engineer', 'data scientist', 'machine learning', 'd\u00e9veloppeur python', 'business developer',
    'software engineer', 'cloud engineer', 'devops', 'ai', 'd\u00e9veloppeur backend',
    'd\u00e9veloppeur frontend', 'backend developer', 'frontend developer', 'python developer'
]

def map_title(title):
    title_lower = str(title).lower()
    for job in JOBS:
        if re.fullmatch(rf"{re.escape(job)}", title_lower.strip()):
            return job
    return title

skills_list = [
    "Python", "Java", "JavaScript", "TypeScript", "C++", "C#", "Go", "Rust", "Scala", "PHP", "SQL", "Spark",
    "Hadoop", "Kafka", "Docker", "Kubernetes", "AWS", "GCP", "Azure", "TensorFlow", "PyTorch",
    "React", "Angular", "Node.js", "FastAPI", "Django", "Flask", "Anglais", "Microsoft Office", "Talend"
]

def extract_skills(desc):
    if pd.isna(desc): return []
    return [skill for skill in skills_list if re.search(r'\\b' + re.escape(skill) + r'\\b', str(desc), re.IGNORECASE)]

def detect_skill_group(label):
    backend = ["Python", "Java", "C++", "C#", "Go", "Rust", "Scala", "PHP"]
    cloud = ["AWS", "GCP", "Azure"]
    frontend = ["JavaScript", "React", "Angular", "Node.js", "TypeScript"]
    data = ["SQL", "Spark", "Hadoop", "Kafka"]
    ai = ["TensorFlow", "PyTorch"]
    if label in backend: return "backend"
    elif label in cloud: return "cloud"
    elif label in frontend: return "frontend"
    elif label in data: return "data"
    elif label in ai: return "ai"
    else: return "other"

def insert_without_duplicates(df, table, pk_col, pg_hook):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Création de la table si elle n'existe pas
    columns_def = ", ".join([f"{col} TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_def});")
 
    # Lecture des clés primaires existantes
    try:
        cursor.execute(f"SELECT {pk_col} FROM {table}")
        existing = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"⚠️ Impossible de lire les clés existantes de {table} : {e}")
        existing = set()
    to_insert = df[~df[pk_col].isin(existing)]
 
    if not to_insert.empty:
        cols = ', '.join(to_insert.columns)
        placeholders = ', '.join(['%s'] * len(to_insert.columns))
        insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        data = [tuple(None if pd.isna(x) else x for x in row) for row in to_insert.to_numpy()]
        cursor.executemany(insert_sql, data)
        conn.commit()
 
    cursor.close()
    conn.close()

def to_sql_via_psycopg2(df, table_name, pg_hook):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    columns = ', '.join([f"{col} TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE {table_name} ({columns});")
    cols = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = [
    tuple(None if (x is None or (isinstance(x, float) and pd.isna(x))) else x for x in row)
    for row in df.to_numpy()
]
    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"✅ Table '{table_name}' ins\u00e9r\u00e9e avec succ\u00e8s via psycopg2.")

# --- Fonction principale ---

def transform_and_load_data():
    client = MongoClient("mongodb+srv://nejihadil00:cNUnd94YpHKSHrJg@datalake.4gnswuw.mongodb.net/?retryWrites=true&w=majority&appName=datalake")
    db = client["job_data"]
    adzuna = pd.DataFrame(list(db["adzuna_jobs"].find()))
    github = pd.DataFrame(list(db["github_trending"].find()))
    glassdoor = pd.DataFrame(list(db["glassdoor"].find()))
    meteojobs = pd.DataFrame(list(db["meteojob"].find()))
    stepstone = pd.DataFrame(list(db["stepstone_jobs"].find()))
    trends = pd.DataFrame(list(db["google_trend"].find()))

    adzuna = adzuna.rename(columns={"poste": "job_title", "entreprise": "company", "lieu": "location", "pays": "country"})
    glassdoor = glassdoor.rename(columns={"entreprise": "company", "lieu": "location", "poste": "job_title", "pays": "country", "salaire": "salary", "note": "rate"})
    stepstone = stepstone.rename(columns={"Company Name": "company", "Location": "location", "Job Title": "job_title", "pays": "country"})
    meteojobs = meteojobs.rename(columns={"job_title": "job_title", "company": "company", "location": "location", "contract_type": "contract_type", "salary": "salary"})

    colonnes_finales = [
        "id", "job_title", "normalized_title", "company", "location", "source", "country", "date",
        "description", "skills", "salary_min", "salary_max", "currency", "contract_type"
    ]

    for df, src in zip([adzuna, glassdoor, stepstone, meteojobs], ["adzuna", "glassdoor", "stepstone", "meteojobs"]):
        df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["source"] = src
        df["date"] = pd.to_datetime("today").date()
        if "country" not in df.columns:
            df["country"] = "fr"
        for col in colonnes_finales:
            if col not in df.columns:
                df[col] = np.nan
        if "salary" in df.columns:
            df[["salary_min", "salary_max"]] = df["salary"].apply(lambda x: pd.Series(normalize_salary(x)))
        df["currency"] = "EUR"
        df["contract_type"] = df["job_title"].apply(detect_contract)
        df["normalized_title"] = df["job_title"].apply(extract_normalized_title).apply(map_title)
        df["skills"] = df["description"].apply(extract_skills)

    job_offers = pd.concat([adzuna[colonnes_finales], glassdoor[colonnes_finales], stepstone[colonnes_finales], meteojobs[colonnes_finales]], ignore_index=True)
    job_offers["country"] = job_offers["country"].str.strip().str.upper()
    job_offers = job_offers[(job_offers["salary_min"].isna()) | (job_offers["salary_min"] <= 250000)]

    d_country = job_offers[["country"]].drop_duplicates().reset_index(drop=True)
    d_country["id_country"] = d_country.index + 1
    d_country["iso2"] = d_country["country"].str[:2]
    d_country["country_name"] = d_country["country"]
    d_country["monnaie_iso3"] = "EUR"

    d_date = job_offers[["date"]].drop_duplicates().rename(columns={"date": "date_key"}).reset_index(drop=True)
    d_date["day"] = pd.to_datetime(d_date["date_key"]).dt.day
    d_date["month"] = pd.to_datetime(d_date["date_key"]).dt.month
    d_date["quarter"] = pd.to_datetime(d_date["date_key"]).dt.quarter
    d_date["year"] = pd.to_datetime(d_date["date_key"]).dt.year
    d_date["day_week"] = pd.to_datetime(d_date["date_key"]).dt.weekday + 1

    d_source = job_offers[["source"]].drop_duplicates().rename(columns={"source": "source_name"}).reset_index(drop=True)
    d_source["id_source"] = d_source.index + 1

    d_skill = job_offers.explode("skills")[["skills"]].drop_duplicates().rename(columns={"skills": "tech_label"}).reset_index(drop=True)
    d_skill = d_skill[d_skill["tech_label"].notna() & (d_skill["tech_label"] != "")]
    d_skill["skill_group"] = d_skill["tech_label"].apply(detect_skill_group)
    d_skill["id_skill"] = d_skill.index + 1

    salary_by_country = job_offers.dropna(subset=["salary_min", "salary_max"])[["id", "country", "normalized_title", "salary_min", "salary_max", "currency", "date"]].copy()
    salary_by_country["salary_median"] = salary_by_country[["salary_min", "salary_max"]].median(axis=1)
    salary_by_country = salary_by_country.merge(d_country[["country", "country_name"]], on="country", how="left")
    salary_by_country = salary_by_country[["id", "country_name", "normalized_title", "salary_min", "salary_max", "salary_median", "currency", "date"]]

    rating_by_company = glassdoor[["id", "company", "job_title", "rate", "date"]].copy()
    rating_by_company = rating_by_company.rename(columns={"job_title": "normalized_title", "rate": "rating"})
    rating_by_company["rating"] = rating_by_company["rating"].astype(str).str.replace(",", ".").replace("Non not\u00e9", np.nan)
    rating_by_company["rating"] = pd.to_numeric(rating_by_company["rating"], errors="coerce")
    rating_by_company = rating_by_company.dropna(subset=["rating"])
    rating_by_company = rating_by_company[["id", "company", "normalized_title", "rating", "date"]]

    github["id"] = [str(uuid.uuid4()) for _ in range(len(github))]
    github["stars"] = github["stars"].astype(str).str.replace(",", "").astype(int)
    github["stars_period"] = github["stars_period"].astype(str).str.replace(",", "").astype(int)
    github["date"] = pd.to_datetime("today").date()
    github_projects = github[["id", "author", "name", "description", "language", "stars", "stars_period", "url", "date"]]

    trends["id"] = [str(uuid.uuid4()) for _ in range(len(trends))]
    trends["country"] = trends["country"].str.strip().str.upper()
    google_trends = trends.rename(columns={"keyword": "keyword"})[["id", "date", "keyword", "country", "timeframe", "popularity"]]
    google_trends = google_trends.merge(d_country[["country", "country_name"]], on="country", how="left")
    google_trends = google_trends[["id", "date", "keyword", "country_name", "timeframe", "popularity"]]

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    insert_without_duplicates(d_country, "d_country", "country_name", pg_hook)
    insert_without_duplicates(d_date, "d_date", "date_key", pg_hook)
    insert_without_duplicates(d_source, "d_source", "source_name", pg_hook)
    insert_without_duplicates(d_skill, "d_skill", "tech_label", pg_hook)

    to_sql_via_psycopg2(job_offers, "job_offers", pg_hook)
    to_sql_via_psycopg2(salary_by_country, "salary_by_country", pg_hook)
    to_sql_via_psycopg2(rating_by_company, "rating_by_company", pg_hook)
    to_sql_via_psycopg2(github_projects, "github_projects", pg_hook)
    to_sql_via_psycopg2(google_trends, "google_trends", pg_hook)

    logger.info("✅ Tout est termin\u00e9 avec succ\u00e8s!")

if __name__ == "__main__":
    transform_and_load_data()
