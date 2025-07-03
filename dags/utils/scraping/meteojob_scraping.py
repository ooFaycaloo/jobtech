import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36'
}

KEYWORDS = ["data", "developement", "python", "ai"]
LOCATION = "europe"
MAX_PAGES = 85
BASE_URL = "https://www.meteojob.com/jobs?what={}&where={}&page={}"

def extract_offers(soup):
    offers_data = []
    offers = soup.find_all('li', id=lambda x: x and x.startswith('job-offer-'))

    for offer_li in offers:
        title = offer_li.find('h2', class_='cc-job-offer-title')
        company = offer_li.find('p', id=lambda x: x and 'company-name' in x)
        location_div = offer_li.find('div', id=lambda x: x and 'job-locations' in x)
        contract_div = offer_li.find('div', id=lambda x: x and 'contract-types' in x)
        salary_span = offer_li.find('span', class_='cc-tag-primary-light')

        offers_data.append({
            'date': pd.Timestamp.now().isoformat(),
            'position': title.get_text(strip=True) if title else 'N/A',
            'company': company.get_text(strip=True) if company else 'N/A',
            'location': location_div.find('span').get_text(strip=True) if location_div else 'N/A',
            'contract': contract_div.find('span').get_text(strip=True) if contract_div else 'N/A',
            'salary': salary_span.get_text(strip=True) if salary_span else 'N/A'
        })

    return offers_data

def fetch_page(keyword, page):
    url = BASE_URL.format(keyword, LOCATION, page)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        offers = extract_offers(soup)

        if offers:
            logger.info(f"✅ {len(offers)} offres trouvées : {keyword} page {page}")
            return offers
        else:
            logger.info(f"🚫 Aucune offre : {keyword} page {page}")
            return []

    except Exception as e:
        logger.error(f"❌ Erreur sur {url} : {e}")
        return []

def fetch_meteojob_offers() -> pd.DataFrame:
    all_data = []
    max_workers = 10
    tasks = [(kw, p) for kw in KEYWORDS for p in range(1, MAX_PAGES + 1)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {executor.submit(fetch_page, kw, p): (kw, p) for kw, p in tasks}

        for future in as_completed(future_to_task):
            result = future.result()
            if result:
                all_data.extend(result)

    return pd.DataFrame(all_data)

def save_meteojob_offers_to_csv(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"✅ {len(df)} offres MeteoJob sauvegardées dans {output_path}")

def run_meteojob_scraping():
    df = fetch_meteojob_offers()
    save_meteojob_offers_to_csv(df, "/home/fboubekri/data_challenge/output/meteojob.csv")
