import pandas as pd
import uuid
import numpy as np
import re
from sqlalchemy import create_engine
from pymongo import MongoClient
import logging

def etl_job_data_to_postgres():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Connexions
        engine = create_engine("postgresql://fatcal:faycal@localhost:5432/talent_insights")
        client = MongoClient("mongodb+srv://nejihadil00:cNUnd94YpHKSHrJg@datalake.4gnswuw.mongodb.net/?retryWrites=true&w=majority&appName=datalake")
        db = client["job_data"]

        collections = {
            "adzuna": db["adzuna_jobs"],
            "github": db["github_trending"],
            "glassdoor": db["glassdor"],
            "meteojobs": db["meteojob"],
            "stepstone": db["stepstone_jobs"],
            "google_trend": db["google_trend"]
        }

        logger.info("✅ Connexions établies avec MongoDB et PostgreSQL")

        # Chargement des données
        adzuna = pd.DataFrame(list(collections["adzuna"].find()))
        github = pd.DataFrame(list(collections["github"].find()))
        glassdoor = pd.DataFrame(list(collections["glassdoor"].find()))
        meteojobs = pd.DataFrame(list(collections["meteojobs"].find()))
        stepstone = pd.DataFrame(list(collections["stepstone"].find()))
        trends = pd.DataFrame(list(collections["google_trend"].find()))

        # Harmonisation
        adzuna = adzuna.rename(columns={"poste": "job_title", "entreprise": "company", "lieu": "location", "pays": "country"})
        glassdoor = glassdoor.rename(columns={"entreprise": "company", "lieu": "location", "poste": "job_title", "pays": "country", "salaire": "salary", "note": "rate"})
        stepstone = stepstone.rename(columns={"Company Name": "company", "Location": "location", "Job Title": "job_title", "pays": "country"})
        meteojobs = meteojobs.rename(columns={"job_title": "job_title", "company": "company", "location": "location", "contract_type": "contract_type", "salary": "salary"})

        for df, src in zip([adzuna, glassdoor, stepstone, meteojobs], ["adzuna", "glassdoor", "stepstone", "meteojobs"]):
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
            df["source"] = src
            df["date"] = pd.to_datetime("today").date()
            if "country" not in df.columns:
                df["country"] = "fr"

        colonnes_finales = [
            "id", "job_title", "normalized_title", "company", "location", "source", "country", "date",
            "description", "skills", "salary_min", "salary_max", "currency", "contract_type"
        ]

        for df_name, df in zip(["adzuna", "glassdoor", "stepstone", "meteojobs"], [adzuna, glassdoor, stepstone, meteojobs]):
            for col in colonnes_finales:
                if col not in df.columns:
                    logger.warning(f"La colonne '{col}' est absente de {df_name}. Elle sera remplie avec des valeurs vides.")
                    df[col] = np.nan

        def normalize_salary(text):
            if pd.isna(text) or str(text).strip() == "-1":
                return None, None
            text = str(text).lower().replace(",", ".").replace("€", "").replace("k", "000")
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

        for df in [adzuna, glassdoor, stepstone, meteojobs]:
            if "salary" in df.columns:
                df[["salary_min", "salary_max"]] = df["salary"].apply(lambda x: pd.Series(normalize_salary(x)))
            df["currency"] = "EUR"

        def detect_contract(job_title):
            job_title = str(job_title).lower()
            if "alternance" in job_title or "alternant" in job_title: return "alternance"
            elif "stage" in job_title or "stagiaire" in job_title: return "stage"
            elif "cdd" in job_title: return "cdd"
            elif "mission" in job_title: return "mission"
            else: return "cdi"

        for df in [adzuna, glassdoor, stepstone, meteojobs]:
            df["contract_type"] = df["job_title"].apply(detect_contract)

        def clean_title(text):
            return re.sub(r'\b(alternance|alternant|stage|stagiaire|cdi|cdd|mission|freelance|intérim)\b', '', str(text), flags=re.IGNORECASE).strip()

        for df in [adzuna, glassdoor, stepstone, meteojobs]:
            df["job_title"] = df["job_title"].apply(clean_title)

        JOBS = ['data engineer', 'data scientist', 'machine learning', 'python', 'developer', 'software engineer', 'cloud', 'devops', 'ai', 'backend', 'frontend']

        def map_title(title):
            title_lower = str(title).lower()
            for job in JOBS:
                if re.search(rf'\b{re.escape(job)}\b', title_lower):
                    return job
            return title

        for df in [adzuna, glassdoor, stepstone, meteojobs]:
            df["normalized_title"] = df["job_title"].apply(map_title)

        skills_list = ["Python", "Java", "JavaScript", "TypeScript", "C++", "C#", "Go", "Rust", "Scala", "PHP", "SQL", "Spark", "Hadoop", "Kafka", "Docker", "Kubernetes", "AWS", "GCP", "Azure", "TensorFlow", "PyTorch", "React", "Angular", "Node.js", "FastAPI", "Django", "Flask", "Anglais", "Microsoft Office", "Talend"]

        def extract_skills(desc):
            if pd.isna(desc): return []
            return [skill for skill in skills_list if re.search(r'\b' + re.escape(skill) + r'\b', str(desc), re.IGNORECASE)]

        for df in [adzuna, glassdoor, stepstone, meteojobs]:
            df["skills"] = df["description"].apply(extract_skills)

        job_offers = pd.concat([
            adzuna[colonnes_finales],
            glassdoor[colonnes_finales],
            stepstone[colonnes_finales],
            meteojobs[colonnes_finales]
        ], ignore_index=True)

        job_offers = job_offers[(job_offers["salary_min"].isna()) | (job_offers["salary_min"] <= 250000)]

        salary_by_country = job_offers.dropna(subset=["salary_min", "salary_max"])[
            ["id", "country", "normalized_title", "salary_min", "salary_max", "currency", "date"]
        ]
        salary_by_country["salary_median"] = salary_by_country[["salary_min", "salary_max"]].median(axis=1)

        rating_by_company = glassdoor[["id", "company", "job_title", "rate", "date"]].copy()
        rating_by_company = rating_by_company.rename(columns={
            "job_title": "normalized_title",
            "rate": "rating"
        })
        rating_by_company["rating"] = rating_by_company["rating"].astype(str).str.replace(",", ".").replace("Non noté", np.nan)
        rating_by_company["rating"] = pd.to_numeric(rating_by_company["rating"], errors="coerce")
        rating_by_company = rating_by_company.dropna(subset=["rating"])

        github["id"] = [str(uuid.uuid4()) for _ in range(len(github))]
        github["stars"] = github["stars"].astype(str).str.replace(",", "").astype(int)
        github["stars_period"] = github["stars_period"].astype(str).str.replace(",", "").astype(int)
        github["date"] = pd.to_datetime("today").date()
        github_projects = github[[
            "id", "author", "name", "description", "language",
            "stars", "stars_period", "url", "date"
        ]]

        trends["id"] = [str(uuid.uuid4()) for _ in range(len(trends))]
        google_trends = trends[[
            "id", "date", "keyword", "country", "timeframe", "popularity"
        ]]

        salary_by_country.to_sql("salary_by_country", engine, if_exists="replace", index=False)
        rating_by_company.to_sql("rating_by_company", engine, if_exists="replace", index=False)
        github_projects.to_sql("github_projects", engine, if_exists="replace", index=False)
        google_trends.to_sql("google_trends", engine, if_exists="replace", index=False)

        logger.info("✅ Tables salary_by_country, rating_by_company, github_projects et google_trends créées avec succès.")

    except Exception as e:
        logger.error(f"❌ Une erreur s'est produite : {e}")
