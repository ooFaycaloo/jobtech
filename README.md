# Talent Insight Project

## Overview
The Talent Insight project is designed to scrape, clean, and analyze job-related data from various online sources. It leverages Apache Airflow for orchestrating ETL (Extract, Transform, Load) workflows and integrates with MongoDB and PostgreSQL for data storage and analysis.

## Project Structure

### Folders
- **dags/**: Contains Airflow DAGs (Directed Acyclic Graphs) for orchestrating scraping and ETL tasks.
  - `dag.py`: Main DAG file defining tasks for scraping, cleaning, and storing data.
  - **utils/**: Utility scripts for scraping, cleaning, and database operations.
    - `mongo.py`: Functions for interacting with MongoDB.
    - `cleaning.py`: Functions for cleaning scraped data and add it to potgres.
    - **scraping/**: Scripts for scraping data from various sources.
      - `glassdor_scraping.py`: Scrapes job data from Glassdoor.
      - `azuna_scaping.py`: Scrapes job data from Adzuna.
      - `github_trending_scraping.py`: Scrapes trending repositories from GitHub.
      - `google_trend.py`: Scrapes Google Trends data.
      - `meteojob_scraping.py`: Scrapes job data from Meteojob.

- **env/**: Python virtual environment for managing dependencies.
- **output/**: Stores intermediate and final CSV files generated during the ETL process.

### Sources of Data
- **Glassdoor**: Scraped using Selenium.
- **Adzuna**:  using HTTP requests.
- **GitHub Trending**: Scraped Selenium.
- **Meteojob**: Scraped using Selenium.
- **Google Trends**: Scraped using HTTP requests.
- **Stepstone**: Data provided directly as a CSV file (`stepstone_jobs.csv`).



## Features
- **Scraping**: Collects job data from multiple platforms like Glassdoor, Adzuna, Meteojob, and Google Trends.
- **Data Cleaning**: Cleans and preprocesses scraped data for analysis.
- **Database Integration**: Inserts cleaned data into MongoDB and PostgreSQL for storage and further analysis.
- **ETL Orchestration**: Uses Apache Airflow to automate and schedule scraping and data processing tasks.

## API Jobs
The `api_jobs` folder contains a Django-based REST API for interacting with job data stored in the database. Key components include:
- **Models**: Define the structure of job-related data.
- **Views**: Handle API requests and responses.
- **Serializers**: Convert data between JSON and database formats.
- **URLs**: Define API endpoints.

### How to Run the API
1. Navigate to the `api_jobs` directory:
   ```bash
   cd api_jobs
   ```
2. Run the Django development server:
   ```bash
   python manage.py runserver
   ```
3. Access the API at `http://localhost:8000`.

## How to Use

### Prerequisites
- requirements.txt

### Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd talent_insight
   ```
2. Set up the Python virtual environment:
   ```bash
   source env/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure Airflow:
   ```bash
   airflow db init
   airflow users create -u admin -p admin -r Admin -e admin@example.com -f Admin -l User
   ```
5. Start the Airflow webserver:
   ```bash
   airflow webserver
   ```
6. Start the Airflow scheduler:
   ```bash
   airflow scheduler
   ```

### Running the DAG
1. Access the Airflow web interface at `http://localhost:8080`.
2. Trigger the `talent_insights` DAG to start scraping and processing data.

## Output
- Scraped data is stored in the `output/` folder as CSV files then in the Mongodb as datalake.
- Cleaned and processed data is inserted into PostgreSQL for further analysis.

