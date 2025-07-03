from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import csv
import time
import pandas as pd
import os
from multiprocessing import Pool, cpu_count


from selenium.common.exceptions import (
    NoSuchElementException, ElementNotInteractableException, TimeoutException, ElementClickInterceptedException
)
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def close_login_popup(driver, timeout=5):
    try:
        wait = WebDriverWait(driver, timeout)
        close_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'CloseButton')))
        close_button.click()
        logger.info("Popup de connexion fermée.")
        time.sleep(1)
    except TimeoutException:
        logger.info("Popup de connexion non présente.")
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        logger.warning(f"Erreur en tentant de fermer la popup : {e}")


def scroll_and_click_load_more(driver, pause_time=2, max_attempts=1000, scroll_offset=200):
    last_count = 0
    attempts = 0

    while attempts < max_attempts:
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        scroll_position = scroll_height - scroll_offset
        driver.execute_script(f"window.scrollTo(0, {scroll_position});")
        time.sleep(pause_time)

        try:
            load_more_button = driver.find_element(By.XPATH, "//button[contains(., \"Voir plus d'offres d'emplois\")]")
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                load_more_button.click()
                logger.info("Bouton 'Voir plus d'offres d'emplois' cliqué")
                time.sleep(pause_time)
                close_login_popup(driver)
            else:
                logger.info("Bouton 'Voir plus' non cliquable")
                break
        except (NoSuchElementException, ElementNotInteractableException):
            logger.info("Bouton 'Voir plus' non trouvé ou non interactif, fin du chargement")
            break

        jobs_cards = driver.find_elements(By.CSS_SELECTOR, "li.react-job-listing")
        current_count = len(jobs_cards)

        if current_count > last_count:
            last_count = current_count
            attempts = 0
        else:
            attempts += 1

    logger.info(f"Chargement terminé, nombre total d'offres : {last_count}")



def export_to_csv(data, filename='/home/fboubekri/data_challenge/output/output.csv'):
    if not data:
        logger.warning("Aucune donnée à exporter.")
        return

    keys = data[0].keys()
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            if not file_exists or os.stat(filename).st_size == 0:
                writer.writeheader()
            writer.writerows(data)
        logger.info(f"{len(data)} lignes ajoutées dans {filename}")
    except Exception as e:
        logger.error(f"Erreur lors de l'export CSV vers {filename} : {e}")


def get_driver():
    options = Options()
    # options.add_argument("--headless")  # Active si tu veux sans UI
    options.add_argument("--start-maximized")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def accept_cookies(driver):
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accepter')]"))
        )
        btn.click()
        logger.info("Bandeau cookies accepté.")
    except:
        logger.info("Aucun bandeau cookies détecté.")


def extract_jobs(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[data-test="job-title"]'))
        )
        titles = driver.find_elements(By.CSS_SELECTOR, 'a[data-test="job-title"]')
        logger.info(f"{len(titles)} offres détectées.")

        jobs = []

        for title_elem in titles:
            try:
                card = title_elem.find_element(By.XPATH, "./ancestor::div[1]")
                title = title_elem.text

                try:
                    location = card.find_element(By.CSS_SELECTOR, 'div[data-test="emp-location"]').text
                except:
                    location = "Non renseigné"

                try:
                    company = card.find_element(By.CSS_SELECTOR, '.EmployerProfile_compactEmployerName__9MGcV').text
                except:
                    company = "Non renseignée"

                try:
                    salary = card.find_element(By.CSS_SELECTOR, 'div[data-test="detailSalary"]').text
                except:
                    salary = "Non renseigné"

                try:
                    rating = card.find_element(By.CSS_SELECTOR, '.rating-single-star_RatingText__XENmU').text
                except:
                    rating = "Non noté"

                try:
                    description = card.find_element(By.CSS_SELECTOR, 'div.JobCard_jobDescriptionSnippet__l1tnl').text
                except:
                    description = "Non renseignée"

                jobs.append({
                    "poste": title,
                    "lieu": location,
                    "entreprise": company,
                    "salaire": salary,
                    "note": rating,
                    "description": description
                })

            except Exception as e:
                logger.warning(f"Erreur lors du parsing d'une offre : {e}")

        return jobs

    except Exception as e:
        logger.error(f"Erreur d'extraction des titres : {e}")
        return []
    
def scrape_jobs_multiprocess(params):
    url, output_path = params
    driver = get_driver()
    try:
        driver.get(url)
        logger.info(f"Page chargée : {url}")
        accept_cookies(driver)
        scroll_and_click_load_more(driver)
        jobs = extract_jobs(driver)
        logger.info(f"{len(jobs)} offres extraites.")
        export_to_csv(jobs, filename=output_path)
    except Exception as e:
        logger.error(f"Erreur scraping {url} : {e}")
    finally:
        driver.quit()
        

def parallel_scraping(urls, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    tasks = [(url, os.path.join(output_dir, f"jobs_{i}.csv")) for i, url in enumerate(urls)]

    with Pool(processes=min(len(tasks), 4)) as pool:
        pool.map(scrape_jobs_multiprocess, tasks)



def scrape_jobs(URL):
    driver = get_driver()
    try:
        driver.get(URL)
        logger.info(f"Page chargée : {URL}")
        accept_cookies(driver)
        scroll_and_click_load_more(driver)
        jobs = extract_jobs(driver)

        for job in jobs:
            logger.info(job)

        logger.info(f"{len(jobs)} offres extraites.")
        export_to_csv(jobs)
    finally:
        driver.quit()

def concat_csv_files(input_dir: str, output_file: str):
    """
    Concatène tous les fichiers CSV commençant par 'jobs' dans un répertoire,
    puis les supprime après avoir généré le fichier final.
    """
    import glob
    import os

    # Trouve les fichiers CSV commençant par 'jobs'
    csv_files = glob.glob(os.path.join(input_dir, "jobs*.csv"))

    if not csv_files:
        logger.warning("Aucun fichier 'jobs*.csv' trouvé à concaténer.")
        return

    logger.info(f"📄 {len(csv_files)} fichiers trouvés pour concaténation : {csv_files}")

    try:
        # Concaténation
        df_list = [pd.read_csv(f) for f in csv_files]
        final_df = pd.concat(df_list, ignore_index=True)
        final_df.to_csv(output_file, index=False)
        logger.info(f"✅ Fichier final sauvegardé dans : {output_file}")

        # Suppression des fichiers après concaténation
        for f in csv_files:
            os.remove(f)
            logger.info(f"🗑️ Supprimé : {f}")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la concaténation ou suppression : {e}")
