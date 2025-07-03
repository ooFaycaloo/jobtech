# utils/scraping/github_trending.py

import requests
from bs4 import BeautifulSoup
import csv
import os
OUTPUT_DIR = "/home/fboubekri/data_challenge/output"

def get_trending_url(period="daily"):
    base_url = "https://github.com/trending"
    if period in ["daily", "weekly", "monthly"]:
        return f"{base_url}?since={period}"
    return base_url

def scrape_github_trending(period="daily"):
    url = get_trending_url(period)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    repo_list = soup.find_all("article", class_="Box-row")

    data = []
    for repo in repo_list:
        full_name = repo.h2.text.strip().replace("\n", "").replace(" ", "")
        author, name = (full_name.split("/") + [""])[:2]

        description_tag = repo.find("p", class_="col-9 color-fg-muted my-1 pr-4")
        description = description_tag.text.strip() if description_tag else ""

        lang_tag = repo.find("span", itemprop="programmingLanguage")
        language = lang_tag.text.strip() if lang_tag else ""

        stars_tag = repo.find("a", href=lambda x: x and x.endswith("/stargazers"))
        stars = stars_tag.text.strip().replace(",", "") if stars_tag else "0"

        stars_period_tag = repo.find("span", class_="d-inline-block float-sm-right")
        stars_period = stars_period_tag.text.strip().split(" ")[0] if stars_period_tag else "0"

        repo_url = "https://github.com" + repo.h2.a["href"]

        data.append({
            "author": author,
            "name": name,
            "description": description,
            "language": language,
            "stars": int(stars),
            "stars_period": stars_period,
            "url": repo_url
        })

    return data

def save_to_csv(data, filename="github_trending.csv"):
    if not data:
        return

    keys = data[0].keys()
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def run_github_scraping():
    data = scrape_github_trending(period="daily")
    save_to_csv(data, os.path.join(OUTPUT_DIR, "github_trending.csv"))
