import requests
from bs4 import BeautifulSoup
import datetime
import json
import logging
import re
import pandas as pd
import os
import traceback
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date, Text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dateutil import parser
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.jobs.nhs.uk/candidate/search/results?workingPattern=full-time&contractType=Permanent&payRange=30-40%2C40-50%2C50-60%2C60-70%2C70-80%2C80-90%2C90-100%2C100&language=en#"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
PARAMS = {
    'searchFormType': 'main',
    'searchByLocationOnly': 'true',
    'language': 'en'
}
OUTPUT_FILE = "nhs_jobs.json"

# SQLAlchemy setup
Base = declarative_base()
engine = None
Session = None

# Define the database model - Using Text type for potentially large fields
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
    description = Column(Text, nullable=True)  # Added description column
    data_source = Column(String(180), nullable=False)

# Telegram setup
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []

def init_db():
    """Initialize SQLAlchemy engine and create tables if they don't exist"""
    global engine, Session
    try:
        DATABASE_URL = "postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"
        
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Check if table exists
        inspector = inspect(engine)
        if not inspector.has_table('jobs'):
            logger.info("Creating jobs table...")
            Base.metadata.create_all(engine)
            logger.info("Table created successfully")
        else:
            logger.info("Jobs table already exists")
            
        # Test connection
        with engine.connect() as conn:
            logger.info("Database connection test successful")
            
    except Exception as e:
        error_message = f"Failed to initialize database: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "init_db")
        raise

def get_chat_id(TOKEN: str) -> str:
    """Fetches the chat ID from the latest Telegram bot update."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = requests.get(url)
        info = response.json()
        chat_id = info['result'][0]['message']['chat']['id']
        return chat_id
    except Exception as e:
        logger.error(f"Failed to get chat ID: {e}")
        return None

def send_message(TOKEN: str, message: str, chat_id: str):
    """Sends a message using Telegram bot."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        if response.status_code == 200:
            logger.info("Sent message successfully")
        else:
            logger.error(f"Message not sent. Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def notify_failure(error_message, location="Unknown"):
    """Sends a failure notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ NHS JOBS SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def clean_name(name):
    """Cleans a company name for matching."""
    try:
        name = str(name).strip().lower()
        name = re.sub(r'[^a-z0-9\s]', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name
    except Exception as e:
        error_message = f"Failed to clean name: {str(e)}"
        notify_failure(error_message, "clean_name")
        return ""

def get_company_list():
    """Loads and cleans the list of target companies from CSV."""
    global company_list
    try:
        df = pd.read_csv(r"data/2025-04-04_-_Worker_and_Temporary_Worker.csv")
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Organisation Name'])
        logger.info(f"Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise

def is_matching_company(employer):
    """Checks if the employer matches any company in our target list."""
    try:
        clean_employer = clean_name(employer)
        if clean_employer in company_list:
            return True
        return False
    except Exception as e:
        error_message = f"Failed to check company match: {str(e)}"
        notify_failure(error_message, "is_matching_company")
        return False

def parse_date(date_str):
    """Parse date string in various formats to a datetime.date object"""
    try:
        if not date_str or date_str == "N/A" or date_str == "Not specified" or date_str == "Invalid date format":
            return datetime.now().date()
        
        # Handle "X days ago" format
        days_ago_match = re.search(r'(\d+)d ago', date_str)
        if days_ago_match:
            days = int(days_ago_match.group(1))
            return (datetime.now() - pd.Timedelta(days=days)).date()
            
        # Handle "Today" and "Just posted"
        if date_str.lower() in ["today", "just posted"]:
            return datetime.now().date()
            
        # Handle date strings in "dd/mm/yyyy" format
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                day, month, year = map(int, parts)
                return datetime(year, month, day).date()
        
        # Try parsing with dateutil parser as fallback
        return parser.parse(date_str).date()
    except Exception:
        # If parsing fails, return current date
        return datetime.now().date()

def extract_description(url):
    """Extract job description HTML from the job detail page."""
    try:
        logger.info(f"Fetching job description from: {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the specific class you mentioned
        description_element = soup.find('div', {'class': 'nhsuk-grid-column-two-thirds wrap-paragraphs'})
        
        if description_element:
            # Remove any script tags
            for script in description_element.find_all('script'):
                script.decompose()
                
            # Return the full HTML content as a string
            return str(description_element)
        
        # Fallback to other potential description containers
        description_section = soup.find('section', {'id': 'job-overview'})
        if description_section:
            for script in description_section.find_all('script'):
                script.decompose()
            return str(description_section)
            
        # If no description found
        return "<div>Description not available</div>"
    except Exception as e:
        error_message = f"Failed to extract job description: {str(e)}"
        logger.error(error_message)
        return "<div>Failed to retrieve description</div>"

def truncate_string(text, max_length):
    """Safely truncate a string to specified maximum length."""
    if text and len(text) > max_length:
        return text[:max_length-3] + "..."
    return text

def insert_jobs_to_db(job_listings):
    """Delete all jobs with data_source='nhs' and insert new jobs"""
    if not job_listings:
        logger.info("No jobs to insert into database")
        return 0
    
    inserted_count = 0
    
    try:
        session = Session()
        try:
            # Delete all existing records with data_source='nhs'
            logger.info("Deleting all existing NHS jobs from database...")
            deleted_count = session.query(Job).filter(Job.data_source == 'nhs').delete()
            logger.info(f"Deleted {deleted_count} existing NHS jobs")
            
            # Insert new records
            logger.info(f"Inserting {len(job_listings)} new jobs...")
            for job in job_listings:
                try:
                    # Parse the posted date
                    posted_date = parse_date(job.get('posting_date', ''))
                    
                    # Truncate strings to fit database columns
                    job_title = truncate_string(job.get('title', ''), 255)
                    company_name = truncate_string(job.get('employer', ''), 255)
                    salary = truncate_string(job.get('salary', ''), 100)
                    experience = truncate_string(f"{job.get('job_type', '')} - {job.get('contract_type', '')}", 100)
                    location = truncate_string(job.get('location', ''), 255)

                    if ('hour' in salary.lower()) or ('day' in salary.lower()):
                        continue
                    
                    # Get job description
                    description = job.get('description', 'Description not provided')

                    new_job = Job(
                        job_title=job_title,
                        company_name=company_name,
                        company_logo='',
                        salary=salary,
                        posted_date=posted_date,
                        experience=experience,
                        location=location,
                        apply_link=job.get('url', ''),
                        description=description,  # Add the description
                        data_source='nhs'
                    )
                    
                    session.add(new_job)
                    inserted_count += 1
                except Exception as e:
                    logger.error(f"Error processing job object: {str(e)}")
            
            # Commit the changes
            session.commit()
            logger.info(f"Successfully inserted {inserted_count} new jobs")
            
        except Exception as e:
            error_message = f"Database operation error: {str(e)}"
            logger.error(error_message)
            notify_failure(error_message, "insert_jobs_to_db")
            session.rollback()
        finally:
            session.close()
    except Exception as e:
        error_message = f"Database session error: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "insert_jobs_to_db")
    
    return inserted_count

def scrape_all_pages():
    """Scrape all pages of job listings from the NHS jobs website."""
    all_jobs = []
    page = 1
    matched_jobs = []
    
    logger.info("Starting to scrape NHS job listings")
    while True:
        params = PARAMS.copy()
        params['page'] = str(page)
        
        try:
            logger.info(f"Fetching page {page}")
            response = requests.get(BASE_URL, params=params, headers=HEADERS)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            jobs = parse_jobs(soup)
            
            if not jobs:
                logger.info(f"No more jobs found on page {page}. Stopping.")
                break
            
            logger.info(f"Found {len(jobs)} jobs on page {page}")
            all_jobs.extend(jobs)
            
            # Filter for target companies
            for job in jobs:
                if job.get('is_target_company', False):
                    # Fetch and add the job description
                    job_url = job.get('url', '')
                    if job_url:
                        # Add delay to avoid being rate-limited
                        time.sleep(1)
                        job['description'] = extract_description(job_url)
                    matched_jobs.append(job)
            
            page += 1
            
            # # Break after first page for testing
            # if page > 10:
            #     break
            
        except requests.RequestException as e:
            error_message = f"Error fetching page {page}: {str(e)}"
            logger.error(error_message)
            notify_failure(error_message, f"scrape_all_pages (page {page})")
            break
    
    all_jobs_count = len(all_jobs)
    matched_jobs_count = len(matched_jobs)
    logger.info(f"Total jobs scraped: {all_jobs_count}, Matching target companies: {matched_jobs_count}")
    return all_jobs, matched_jobs

def parse_jobs(soup):
    """Parse job listings from a BeautifulSoup object."""
    job_listings = soup.find_all('li', class_='search-result')
    if not job_listings:
        return []
    
    jobs = []
    current_date = datetime.now().date()
    
    for job in job_listings:
        try:
            title_element = job.find('a', {'data-test': 'search-result-job-title'})
            if not title_element:
                continue
                
            title = title_element.text.strip()
            url = "https://www.jobs.nhs.uk" + title_element['href']
            
            employer_element = job.find('h3', class_='nhsuk-u-font-weight-bold')
            employer = employer_element.contents[0].strip() if employer_element else "Unknown Employer"
            
            # Check if this employer matches our company list
            clean_employer = clean_name(employer)
            is_target_company = clean_employer in company_list
            
            location_element = job.find('div', class_='location-font-size')
            location = location_element.text.strip() if location_element else "Unknown Location"
            
            salary_element = job.find('li', {'data-test': 'search-result-salary'})
            salary = "N/A"
            if salary_element:
                salary = salary_element.text.strip().replace('Salary:', '').strip()
                salary = salary.split('a year')[0].strip()
            
            closing_date_element = job.find('li', {'data-test': 'search-result-closingDate'})
            closing_date_str = "N/A"
            if closing_date_element:
                closing_date_str = closing_date_element.text.strip().replace('Closing date:', '').strip()
            
            posting_date_element = job.find('li', {'data-test': 'search-result-publicationDate'})
            posting_date_str = "N/A"
            if posting_date_element:
                posting_date_str = posting_date_element.text.strip().replace('Date posted:', '').strip()
            
            job_id = "N/A"
            job_type = "N/A"
            contract_type = "N/A"
            
            job_id_element = job.find('span', {'data-test': 'search-result-jobId'})
            if job_id_element:
                job_id = job_id_element.text.strip().replace('Job reference:', '').strip()
            
            job_type_element = job.find('li', {'data-test': 'search-result-jobType'})
            if job_type_element:
                job_type = job_type_element.text.strip().replace('Job type:', '').strip()
            
            contract_type_element = job.find('li', {'data-test': 'search-result-contractType'})
            if contract_type_element:
                contract_type = contract_type_element.text.strip().replace('Contract type:', '').strip()
            
            ingestion_time = datetime.utcnow().isoformat()
            
            job_data = {
                'title': title,
                'url': url,
                'employer': employer,
                'location': location,
                'salary': salary,
                'closing_date': closing_date_str,
                'posting_date': posting_date_str,
                'job_id': job_id,
                'job_type': job_type,
                'contract_type': contract_type,
                'is_target_company': is_target_company,
                'apply_link': url,
                'company_logo': '',
                'source': 'nhs',
                'country': 'UK',
                'ingestion_timestamp': ingestion_time
            }
            
            jobs.append(job_data)
            
        except Exception as e:
            error_message = f"Error parsing job: {str(e)}"
            logger.error(error_message)
            notify_failure(error_message, "parse_jobs")
    
    return jobs

def main():
    """Main function to run the scraper."""
    global chat_id, TOKEN
    logger.info("Starting NHS job scraper...")
    
    try:
        # Get Telegram chat ID
        chat_id = get_chat_id(TOKEN)
        if chat_id:
            send_message(TOKEN, f"🚀 NHS scraper started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", chat_id)
        
        # Initialize database
        init_db()
        
        # Load company list
        get_company_list()
        
        # Scrape all job listings
        all_jobs, matched_jobs = scrape_all_pages()
        
        try:
            if len(all_jobs) == 0:
                error_message = "No jobs collected from NHS site"
                logger.error(error_message)
                notify_failure(error_message, "data_collection")
                return
            
            # Insert jobs into database
            inserted_count = insert_jobs_to_db(matched_jobs)
            
            if chat_id:
                success_message = (
                    f"✅ NHS scraper completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"Scraped {len(all_jobs)} jobs\n"
                    f"Matched {len(matched_jobs)} jobs with target companies\n"
                    f"Inserted {inserted_count} jobs into database"
                )
                send_message(TOKEN, success_message, chat_id)
                
        except Exception as e:
            error_message = f"Failed to save data: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_message)
            notify_failure(error_message, "data_saving")
            
    except Exception as e:
        error_message = f"Critical failure in main: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        notify_failure(error_message, "main")

if __name__ == "__main__":
    main()