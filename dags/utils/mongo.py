#////////////////////////////////////////////////////////////////////////////////////////////////
#yG7GaLpf8DkArh1O
import os
import glob
import pandas as pd
from pymongo import MongoClient
import logging


logger = logging.getLogger(__name__)

# Connexion MongoDB Atlas
MONGO_URI = "mongodb+srv://nejihadil00:cNUnd94YpHKSHrJg@datalake.4gnswuw.mongodb.net/?retryWrites=true&w=majority&appName=datalake"
client = MongoClient(MONGO_URI)
db = client["job_data"]

def save_to_mongodb(data: list, collection_name: str):
    """
    Insère une liste de dict dans MongoDB.

    :param data: liste de dicts à insérer
    :param collection_name: nom de la collection MongoDB
    """
    if not data:
        logger.warning("⚠️ Aucune donnée à insérer dans MongoDB.")
        return

    collection = db[collection_name]
    try:
        collection.insert_many(data)
        logger.info(f"✅ {len(data)} documents insérés dans la collection '{collection_name}'")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'insertion MongoDB : {e}")

def concat_and_save_jobs_csv_to_mongodb(input_dir: str, collection_name: str):
    """
    Concatène tous les fichiers CSV commençant par 'jobs' dans input_dir,
    insère dans MongoDB, puis supprime les fichiers.

    :param input_dir: dossier contenant les CSV
    :param collection_name: collection MongoDB cible
    """
    pattern = os.path.join(input_dir, "jobs*.csv")
    files = glob.glob(pattern)

    if not files:
        logger.warning(f"⚠️ Aucun fichier '{pattern}' trouvé pour concaténation.")
        return

    # Lecture et concaténation
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df_list.append(df)
        except Exception as e:
            logger.error(f"Erreur lecture fichier {f} : {e}")

    if not df_list:
        logger.warning("⚠️ Aucun fichier CSV valide trouvé.")
        return

    full_df = pd.concat(df_list, ignore_index=True)
    data_to_insert = full_df.to_dict(orient='records')

    # Insertion MongoDB
    save_to_mongodb(data_to_insert, collection_name)

    # Suppression des fichiers
    for f in files:
        try:
            os.remove(f)
            logger.info(f"🗑️ Fichier supprimé : {f}")
        except Exception as e:
            logger.error(f"Erreur suppression fichier {f} : {e}")


logger = logging.getLogger(__name__)

def insert_csv_to_mongodb(filepath, collection_name):
    client = MongoClient("mongodb+srv://nejihadil00:cNUnd94YpHKSHrJg@datalake.4gnswuw.mongodb.net/?retryWrites=true&w=majority&appName=datalake")
    db = client["job_data"]
    collection = db[collection_name]

    try:
        df = pd.read_csv(filepath)
        if df.empty:
            logger.warning(f"⚠️ Le fichier {filepath} est vide.")
            return

        # Création de la clé unique pour chaque ligne
        df["unique_key"] = df["title"].astype(str) + "_" + df["company"].astype(str) + "_" + df["location"].astype(str) + "_" + df["date_posted"].astype(str)

        # Récupération des clés déjà existantes en base
        existing_keys = set(
            doc["unique_key"]
            for doc in collection.find({}, {"unique_key": 1})
        )

        # Filtrage : on ne garde que les nouvelles lignes
        new_df = df[~df["unique_key"].isin(existing_keys)]

        if new_df.empty:
            logger.info(f"✅ Aucune nouvelle donnée à insérer pour {collection_name}.")
            return

        # Insertion des lignes filtrées (sans le champ unique_key si tu ne veux pas le stocker)
        records = new_df.to_dict(orient="records")
        collection.insert_many(records)

        logger.info(f"✅ {len(records)} nouvelles lignes insérées dans MongoDB : {collection_name}")

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'insertion dans MongoDB ({collection_name}) : {e}")
