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

### Authentication
The API uses JWT Bearer Token authentication.

1. Retrieve a token:
   ```http
   POST http://localhost:8000/api/token/
   Body (JSON):
   {
     "username": "your_username",
     "password": "your_password"
   }
   ```
2. Use this token in subsequent requests:
   ```
   Authorization: Bearer <your_token>
   ```

## Endpoints to Test

### Job Offers
- List job offers:
  ```
  GET http://localhost:8000/api/v1/joboffers/
  ```
- Most demanded jobs:
  ```
  GET http://localhost:8000/api/v1/joboffers/most_demanded/
  ```
- Required skills for a job:
  ```
  GET http://localhost:8000/api/v1/joboffers/skills/?title=data%20engineer
  ```
- Offers by company:
  ```
  GET http://localhost:8000/api/v1/joboffers/by_company/?title=data%20engineer
  ```
- Best salary offers:
  ```
  GET http://localhost:8000/api/v1/joboffers/best_salary/
  ```
- Offers by region:
  ```
  GET http://localhost:8000/api/v1/joboffers/by_region/?title=data%20engineer
  ```
- Salary comparison by region:
  ```
  GET http://localhost:8000/api/v1/joboffers/salary_by_region/?title=data%20engineer
  ```
- Contract type distribution:
  ```
  GET http://localhost:8000/api/v1/joboffers/contracts/?title=data%20engineer
  ```

### Salary Data
- Salary stats:
  ```
  GET http://localhost:8000/api/v1/salary/stats/?title=data%20engineer&country=france
  ```
- Top paid jobs by country:
  ```
  GET http://localhost:8000/api/v1/salary/top_jobs/?country=france
  ```
- Best countries for a job:
  ```
  GET http://localhost:8000/api/v1/salary/top_countries/?title=data%20engineer
  ```
- Salary trend over time:
  ```
  GET http://localhost:8000/api/v1/salary/trends/?title=data%20engineer
  ```

### Rating
- Top rated companies:
  ```
  GET http://localhost:8000/api/v1/ratings/top/
  ```
- Average rating by company:
  ```
  GET http://localhost:8000/api/v1/ratings/average/
  ```

### Google Trends
- Keyword growth:
  ```
  GET http://localhost:8000/api/v1/trends/growth/?keyword=python&country=france
  ```
- Peak interest date:
  ```
  GET http://localhost:8000/api/v1/trends/peak/?keyword=python
  ```
- Top countries for keyword:
  ```
  GET http://localhost:8000/api/v1/trends/top_countries/?keyword=python
  ```

### GitHub Projects
- Top GitHub projects:
  ```
  GET http://localhost:8000/api/v1/github/top/
  ```
- Language popularity:
  ```
  GET http://localhost:8000/api/v1/github/languages/
  ```

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


## PostgreSQL Integration with Airflow

To facilitate the transformation and loading of data into PostgreSQL, the project uses the `PostgresHook` provided by Apache Airflow. This hook allows seamless interaction with the PostgreSQL database, enabling the execution of SQL queries directly from Airflow tasks.

### Configuration
The connection to PostgreSQL is established via the Airflow interface:
1. Navigate to "Admin > Connections" in the Airflow web interface.
2. Create a new connection with the following details:
   - **Conn Id**: A unique identifier for the connection (e.g., `postgres_default`).
   - **Conn Type**: PostgreSQL.
   - **Host**: The hostname of the PostgreSQL server.
   - **Schema**: The database name.
   - **Login**: The username for the database.
   - **Password**: The password for the database.
   - **Port**: The port number (default is 5432).

### Running the DAG
1. Access the Airflow web interface at `http://localhost:8080`.
2. Trigger the `talent_insights` DAG to start scraping and processing data.

## Output
- Scraped data is stored in the `output/` folder as CSV files then in the Mongodb as datalake.
- Cleaned and processed data is inserted into PostgreSQL for further analysis.
