import requests
import json
import os
import re
import pandas as pd
from datetime import datetime
from typing import List

from sqlalchemy import create_engine, Column, Integer, String, Text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Job(Base):
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    job_title = Column(String(255), nullable=False)
    company_name = Column(String(255), nullable=False)
    company_logo = Column(Text, nullable=True)
    salary = Column(String(100), nullable=True)
    posted_date = Column(Text, nullable=False)
    experience = Column(String(100), nullable=True)
    location = Column(String(255), nullable=True)
    apply_link = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    data_source = Column(String(180), nullable=False)

# Global variables
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
engine = None
Session = None
company_list = []

def get_chat_id(TOKEN: str) -> str:
    """Fetches the chat ID from the latest Telegram bot update."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = requests.get(url)
        info = response.json()
        if info.get('result') and len(info['result']) > 0:
            chat_id = info['result'][0]['message']['chat']['id']
            return chat_id
        else:
            print("No chat updates found")
            return None
    except Exception as e:
        print(f"Failed to get chat ID: {e}")
        return None

def send_message(TOKEN: str, message: str, chat_id: str):
    """Sends a message using Telegram bot."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        if response.status_code == 200:
            print("Sent message successfully")
        else:
            print(f"Message not sent. Status code: {response.status_code}")
    except Exception as e:
        print(f"Failed to send message: {e}")

def notify_failure(error_message, location="Unknown"):
    """Sends a failure notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ Reed SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(message):
    """Sends a success notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"✅ Reed SCRAPER SUCCESS at {timestamp}\n{message}"
        send_message(TOKEN, message, chat_id)

def init_db():
    """Initialize SQLAlchemy engine and create tables if they don't exist"""
    global engine, Session
    try:
        DATABASE_URL = "postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"
        
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Check if table exists, create if not
        inspector = inspect(engine)
        if not inspector.has_table('jobs'):
            print("Creating jobs table...")
            Base.metadata.create_all(engine)
            print("Table created successfully")
            notify_success("Database table 'jobs' created successfully")
        else:
            print("Jobs table already exists")
            
        # Test connection
        with engine.connect() as conn:
            print("Database connection test successful")
            notify_success("Database connection established successfully")
            
    except Exception as e:
        error_message = f"Failed to initialize database: {str(e)}"
        print(error_message)
        notify_failure(error_message, "Database Initialization")
        raise

DEFAULT_CSV_PATH = "data/2025-04-04_-_Worker_and_Temporary_Worker.csv"

def clean_name(name: str) -> str:
    """Clean a company name string for comparison."""
    if not isinstance(name, str):
        return ""
    return re.sub(r'[^\w\s]', '', name).lower().strip()

def get_company_list(path: str) -> List[str]:
    """Load and clean company list from CSV file."""
    global company_list
    
    if not os.path.exists(path):
        error_msg = f"CSV file not found: {path}"
        print(error_msg)
        notify_failure(error_msg, "CSV Loading")
        # Try to work without a company list for now
        return []
        
    try:
        df = pd.read_csv(path)
        
        if 'Organisation Name' not in df.columns:
            error_msg = "Required column 'Organisation Name' not found in CSV"
            print(error_msg)
            notify_failure(error_msg, "CSV Processing")
            return []
            
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = [name for name in list(df['Organisation Name']) if name]  # Filter out empty names
        
        print(f"✅ Loaded {len(company_list)} companies from CSV")
        notify_success(f"Loaded {len(company_list)} companies from CSV file")
        return company_list
    except Exception as e:
        error_msg = f"Failed to process CSV file: {str(e)}"
        print(error_msg)
        notify_failure(error_msg, "CSV Processing")
        return []

def fetch_reed_jobs():
    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en;q=0.7',
        'authorization':'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik5rVkNRa1JHUkRaRE56VTNRalUyTlVReU56TXpOVUpGTlRrd05UbENNRVkyTlVORk5VRkRRZyJ9.eyJodHRwczovL3d3dy5yZWVkLmNvLnVrL2FwaS9hdXRoL2lzLXVzZXItdG9rZW4iOnRydWUsImh0dHBzOi8vd3d3LnJlZWQuY28udWsvYXBpL2F1dGgvcmVlZERiVXNlcklkIjozODk2OTQ3MCwiaHR0cHM6Ly93d3cucmVlZC5jby51ay9hcGkvYXV0aC9tZXRob2QiOiJnb29nbGUtb2F1dGgyIiwiaHR0cHM6Ly93d3cucmVlZC5jby51ay9hcGkvYXV0aC9maXJzdE5hbWUiOiJqYW1lcyIsImh0dHBzOi8vd3d3LnJlZWQuY28udWsvYXBpL2F1dGgvbGFzdE5hbWUiOiJkc291emEiLCJodHRwczovL3d3dy5yZWVkLmNvLnVrL2FwaS9hdXRoL2xhc3RMb2dpbiI6MTc0NTc3OTkwNCwiaHR0cHM6Ly93d3cucmVlZC5jby51ay9hcGkvYXV0aC9wZW51bHRpbWF0ZUxvZ2luIjoxNzQ1Nzc4MDczLCJodHRwczovL3d3dy5yZWVkLmNvLnVrL2FwaS9hdXRoL2FjY291bnRSZWdpc3RyYXRpb24iOjE3NDU1NjQwMzUsImh0dHBzOi8vd3d3LnJlZWQuY28udWsvYXBpL2F1dGgvZW1haWwiOiJqb3NodWRzb3V6YTI3QGdtYWlsLmNvbSIsImh0dHBzOi8vd3d3LnJlZWQuY28udWsvYXBpL2F1dGgvaXNFbWFpbFZlcmlmaWVkIjpmYWxzZSwiaHR0cHM6Ly93d3cucmVlZC5jby51ay9hcGkvYXV0aC9pcCI6IjI0MDU6MjAxOmQwNzg6MTg0NTo1MGZlOmYyNDplN2M3OjkwYTAiLCJpc3MiOiJodHRwczovL3NlY3VyZS5yZWVkLmNvLnVrLyIsInN1YiI6ImF1dGgwfHJlZWQ2ZjBjMjYzNTE1N2U0MDljODJkZDcxNDg3YjdjZGM1YiIsImF1ZCI6WyJodHRwczovL3d3dy5yZWVkLmNvLnVrLyIsImh0dHBzOi8vcmVlZC1wcm9kLmV1LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3NDU4MTQwMTUsImV4cCI6MTc0NTgxNTgxNSwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBvZmZsaW5lX2FjY2VzcyIsImF6cCI6IjJRMHA5N3VteVRnT09NNlBONW9hdDM0bmRpSnR0aU5QIn0.ojx0h-DJelur7E1uYz3eMspuIAfr0PWg9vKG9e96zNFsOUxaSKVU2BpdyxqB-IQ_hm-ngSEKIQYFOksW_ivfRQxqbCbBgxMpS9qdcqMrLFnBf9onSzJFhaIsoOxMPgHYI4RwxB2z4sfF5_B_3ALKcKfM4I6GOYlChr7shAl81ltmShhXXJMWBkWZTaXJllasqheiXQC6Oj017YlGivFovIfKOJiyWkZYmrLgvUeR20KwO3Qe2WaNTr8f991GOBucXPkoIBy69OvpkN5lub1cR0d4rxkpPHVlKVtViheuTXcPPmQ_QMJPMPzFlFk7onoS1suCXGwToUWBi6DrbYvVLg',
        'cache': 'no-store',
        'content-type': 'application/json',
        'origin': 'https://www.reed.co.uk',
        'priority': 'u=1, i',
        'referer': 'https://www.reed.co.uk/',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'x-ab-session-id': 'a0dfc179bec04be0a3137eb152f49826',
        'x-correlation-device-id': '287118f4-68d9-45bc-9c5e-50025aca42a8',
        'x-correlation-id': 'f1ac9a36-4537-495f-9f92-2af31e052b3f',
        'x-correlation-session-id': 'b4ed3b35-fe40-409a-bf03-2e4267026102',
        'x-correlation-source-id': 'web-jobseeker-jobs',
        'x-correlation-unique-user-id': '228221c4-70b2-4fc5-961b-f1a45ac8dbe9',
        'x-query-id': '892c8a7b-e5fa-49bc-b72c-ef7d123e2dfe',
    }

    params = {
        'page': '1',
      }

    json_data = {
        'sortBy': 'displayDate',
        'keywords': '',
        'location': {
            'locationId': 0,
            'locationName': 'united kingdom',
        },
        'parentSectorIds': [],
        'proximity': 10,
        'salaryFrom': 30000,
        'salaryTo': 0,
        'perHour': False,
        'perm': False,
        'temp': False,
        'partTime': False,
        'fullTime': True,
        'ouId': 0,
        'recruiterName': '',
        'clientProfileId': 0,
        'isReed': False,
        'agency': False,
        'direct': False,
        'graduate': False,
        'contract': False,
        'hideTrainingJobs': False,
        'remoteWorkingOption': 'notSpecified',
        'pageno': 1,
        'take': 500,
        'dateCreatedOffSet': 'today',
        'seoDirectory': 'full-time',
        'misspeltKeywords': None,
        'skipKeywordSpellcheck': False,
        'visaSponsorship': False,
        'isEasyApply': False,
        'excludeSalaryDescriptions': [],
        'minApplicants': 0,
        'maxApplicants': 0,
    }

    try:
        print("Starting Reed API request...")
        notify_success("Starting job search from Reed API")
        
        response = requests.post(
            'https://api.reed.co.uk/api-bff-jobseeker-jobs/search/',
            params=params,
            headers=headers,
            json=json_data,
            timeout=30  # Added timeout
        )

        if response.status_code != 200:
            error_msg = f"Failed to fetch jobs. Status code: {response.status_code}"
            print(error_msg)
            notify_failure(error_msg, "Reed API")
            return []

        response_data = response.json()
        save_jobs_to_file(response_data, 'reeddata.json')
        notify_success("Received and saved raw job data from Reed API")

        # Navigate through the nested JSON structure
        jobs = []
        if 'result' in response_data:
            result = response_data['result']
            if 'response' in result:
                response_obj = result['response']
                if 'jobSearchResult' in response_obj:
                    job_search = response_obj['jobSearchResult']
                    if 'searchResults' in job_search:
                        search_results = job_search['searchResults']
                        if 'results' in search_results:
                            jobs = search_results['results']

        if not jobs:
            msg = "No jobs found in the response"
            print(msg)
            notify_failure(msg, "Reed API")
            return []

        job_list = []
        filtered_count = 0
        total_count = len(jobs)

        for job in jobs:
            if 'jobDetail' not in job:
                continue
                
            detail = job.get('jobDetail', {})
            company = detail.get('ouName', '')
            title = detail.get('jobTitle', '')
            logo_image = job.get('logoImage')
            company_logo_url = f"https://resources.reed.co.uk/profileimages/logos/thumbs/{logo_image}" if logo_image else None
            
            # Skip jobs with unrealistic salaries
            salary_from = detail.get('salaryFrom')
            salary_to = detail.get('salaryTo')
            
            if salary_from is not None and salary_from <= 28000:
                filtered_count += 1
                continue
                
            if salary_from is not None and salary_from >= 10000.0 and salary_from <= 22000.0:
                salary_from = None
                salary_to = None

            # Construct apply link if missing
            apply_link = detail.get('externalUrl')
            if not apply_link:
                job_title = detail.get('jobTitle', '').lower().replace(' ', '-')
                job_id = detail.get('jobId', '')
                apply_link = f"https://www.reed.co.uk/jobs/{job_title}/{job_id}"

            # Check if company is in our target list
            cleaned_company = clean_name(company)
            if company_list and cleaned_company not in company_list:
                filtered_count += 1
                continue

            # Format salary
            salary_text = ""
            if salary_from is not None and salary_to is not None:
                salary_text = f"£{salary_from:,.0f} to £{salary_to:,.0f}"
            elif salary_from is not None:
                salary_text = f"£{salary_from:,.0f}+"
            elif salary_to is not None:
                salary_text = f"Up to £{salary_to:,.0f}"
            
            job_info = {
                "job_id": detail.get('jobId'),
                "job_title": title,
                "company_name": company,
                "company_logo": company_logo_url,
                "salary": salary_text,
                "posted_date": detail.get('dateCreated'),
                "experience": None,
                "location": detail.get('cityLocation', '') or detail.get('countyLocation', ''),
                "apply_link": apply_link,
                "description": detail.get('jobDescription'),
                "data_source": "reed"
            }
            job_list.append(job_info)

        print(f"Successfully collected {len(job_list)} jobs from Reed ")
        notify_success(f"Successfully processed {len(job_list)} jobs from Reed ")
        return job_list
    
    except Exception as e:
        error_msg = f"Failed to fetch Reed jobs: {str(e)}"
        print(error_msg)
        notify_failure(error_msg, "Reed API")
        return []

def save_jobs_to_file(jobs, filename='reeddata.json'):
    """Save the raw job data to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        print(f"Saved raw job data to {filename}")
    except Exception as e:
        error_msg = f"Failed to save jobs to file: {str(e)}"
        print(error_msg)
        notify_failure(error_msg, "File Save")

def insert_jobs_to_db(jobs_data):
    """Insert scraped jobs into the database."""
    if not engine:
        init_db()
    
    session = Session()
    try:
        count = 0
        print(f"Preparing to insert {len(jobs_data)} jobs into database...")
        notify_success(f"Preparing to insert {len(jobs_data)} jobs into database")
        
        for job_data in jobs_data:
            job = Job(
                job_title=job_data.get('job_title', ''),
                company_name=job_data.get('company_name', ''),
                company_logo=job_data.get('company_logo', None),
                salary=str(job_data.get('salary')) if job_data.get('salary') else None,
                posted_date=job_data.get('posted_date', ''),
                experience=job_data.get('experience', None),
                location=job_data.get('location', ''),
                apply_link=job_data.get('apply_link', ''),
                description=job_data.get('description', None),
                data_source=job_data.get('data_source', 'reed')
            )
            session.add(job)
            count += 1
        
        session.commit()
        print(f"Successfully inserted {count} jobs into the database")
        notify_success(f"Successfully inserted {count} jobs into the database")
        return count
    except Exception as e:
        session.rollback()
        error_message = f"Failed to insert jobs: {str(e)}"
        print(error_message)
        notify_failure(error_message, "Database Insert")
        raise
    finally:
        session.close()

def delete_jobs_by_source(source="reed"):
    """Delete jobs by data source."""
    if not engine:
        init_db()
    
    session = Session()
    try:
        print(f"Deleting existing jobs with source '{source}'...")
        count = session.query(Job).filter(Job.data_source == source).delete()
        session.commit()
        print(f"Successfully deleted {count} jobs with source '{source}'")
        notify_success(f"Successfully deleted {count} existing jobs with source '{source}'")
        return count
    except Exception as e:
        session.rollback()
        error_message = f"Failed to delete jobs: {str(e)}"
        print(error_message)
        notify_failure(error_message, "Database Delete")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    try:
        print("Reed job scraper starting...")
        notify_success("Reed job scraper process started")
        
        # Initialize the database connection
        init_db()
        
        # Load company list - if the CSV doesn't exist, it will continue with an empty list
        company_list = get_company_list(DEFAULT_CSV_PATH)
        
        # Fetch jobs from Reed
        print("Fetching jobs from Reed...")
        jobs = fetch_reed_jobs()
        
        if jobs:
            # Delete existing jobs from this source
            print("Deleting existing Reed jobs...")
            delete_jobs_by_source()
            
            # Insert new jobs
            print("Inserting new jobs...")
            inserted_count = insert_jobs_to_db(jobs)
            
            print(f"Job scraping completed successfully! {inserted_count} jobs processed.")
            notify_success(f"🎉 Job scraping completed successfully! {inserted_count} jobs processed.")
        else:
            print("No jobs were found to insert")
            notify_failure("No jobs were found to insert", "Job Processing")
    except Exception as e:
        error_msg = f"Script execution failed: {str(e)}"
        print(error_msg)
        notify_failure(error_msg, "Main Script")
    finally:
        print("Reed job scraper finished")
