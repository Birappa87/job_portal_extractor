import requests
import json
import urllib.parse
import logging
import re
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from rapidfuzz import process

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("simplyhired_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define SQLAlchemy base
Base = declarative_base()

# Define the database model
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

# Telegram setup
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []

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
        message = f"❌ SIMPLYHIRED SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(job_count):
    """Sends a success notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"✅ SIMPLYHIRED SCRAPER SUCCESS at {timestamp}\nScraped {job_count} jobs successfully."
        send_message(TOKEN, message, chat_id)

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
            
        return True
    except Exception as e:
        error_message = f"Failed to initialize database: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "init_db")
        return False

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

def is_company_match(job_company, target_companies):
    """
    Check if any word in the job company name matches with any word in any target company name.
    Returns True if there's a match, False otherwise.
    """
    try:
        # Clean the job company name
        clean_job_company = clean_name(job_company)
        job_words = set(clean_job_company.split())
        
        # Skip very common/generic words that might cause false matches
        common_words = {'ltd', 'limited', 'llc', 'inc', 'incorporated', 'the', 'and', 'company', 'group', 'services'}
        job_words = job_words - common_words
        
        # If job company has no meaningful words after filtering, return False
        if not job_words:
            return False
        
        # Check for word-level matches with any company in the list
        for target_company in target_companies:
            target_words = set(target_company.split())
            target_words = target_words - common_words
            
            # If any meaningful word matches, return True
            if job_words.intersection(target_words):
                return True
                
        return False
    except Exception as e:
        logger.error(f"Error in company matching: {e}")
        return False

def extract_jobs(job_list):
    """Extract relevant fields from raw job data"""
    extracted = []
    for job in job_list:
        try:
            # Handle apply link
            if job.get('encodedJobClickPingUrl'):
                apply_link = f"https://www.simplyhired.co.uk{urllib.parse.unquote(job.get('encodedJobClickPingUrl'))}"
            else:
                apply_link = f"https://www.simplyhired.co.uk{job.get('botUrl')}"

            company_name = job.get("company", "Unknown")
            salary = job.get("salaryInfo", "")

            match, score, _ = process.extractOne(company_name, company_list)
            if score > 80 and (salary.lower() not in ['hour', 'day', 'hourly']):
                extracted.append({
                    "job_title": job.get("title", ""),
                    "company_name": company_name,
                    "company_logo": None,
                    "salary": job.get("salaryInfo", ""),
                    "posted_date": job.get("dateOnIndeed", datetime.now().strftime("%Y-%m-%d")),
                    "experience": None,
                    "location": job.get("location", ""),
                    "apply_link": apply_link,
                    "description": job.get("snippet", ""),
                    "data_source": "simplyhired"
                })
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            continue
    return extracted

def save_to_db(jobs):
    """Save the extracted jobs to the database after clearing old SimplyHired jobs"""
    session = Session()
    try:
        # Delete existing jobs with data_source 'simplyhired'
        deleted_count = session.query(Job).filter(Job.data_source == 'simplyhired').delete()
        logger.info(f"Deleted {deleted_count} old jobs from 'simplyhired'")

        # Filter only unique jobs by 'apply_link' before insertion
        unique_jobs = []
        seen_links = set()
        for job_data in jobs:
            apply_link = job_data.get("apply_link")
            if apply_link and apply_link not in seen_links:
                seen_links.add(apply_link)
                unique_jobs.append(Job(**job_data))

        session.add_all(unique_jobs)
        session.commit()
        logger.info(f"Inserted {len(unique_jobs)} new jobs to database")
        return len(unique_jobs)
    except Exception as e:
        session.rollback()
        error_message = f"Error saving to database: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "save_to_db")
        return 0
    finally:
        session.close()

def scrape_simplyhired():
    """Main function to scrape SimplyHired jobs"""
    # Define your base setup
    base_url = 'https://www.simplyhired.co.uk/_next/data/K5acjOSV3t1FEn0bZiGoi/en-GB/search.json'

    cookies = {
        'shk': '1ips6rrfjgfu5801',
        '_ga': 'GA1.1.862499209.1745777194',
        'OptanonAlertBoxClosed': '2025-04-27T18:07:27.374Z',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7,kn;q=0.6',
        'priority': 'u=1, i',
        'referer': 'https://www.simplyhired.co.uk/search?l=united+kingdom',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36',
        'x-nextjs-data': '1',
    }

    params_base = {
        'l': 'united kingdom',
        'mip': '30000',
    }

    all_jobs = []
    visited_cursors = set()
    next_cursor = None
    page_count = 0
    
    try:
        logger.info("Starting SimplyHired scraper")
        
        while True:  # Continue until no more pages are available
            params = params_base.copy()
            if next_cursor:
                params["cursor"] = next_cursor
            
            logger.info(f"Fetching page {page_count + 1}")
            response = requests.get(base_url, headers=headers, cookies=cookies, params=params)
            
            if response.status_code != 200:
                error_message = f"Failed to fetch page {page_count + 1}: Status code {response.status_code}"
                logger.error(error_message)
                notify_failure(error_message, f"page_{page_count + 1}")
                break
                
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                error_message = f"Failed to parse JSON on page {page_count + 1}: {str(e)}"
                logger.error(error_message)
                notify_failure(error_message, f"page_{page_count + 1}")
                break
            
            # Extract jobs from current page
            if 'pageProps' in data and 'jobs' in data['pageProps']:
                jobs = data['pageProps']['jobs']
                page_jobs = extract_jobs(jobs)
                all_jobs.extend(page_jobs)
                logger.info(f"Extracted {len(page_jobs)} jobs from page {page_count + 1}")
            else:
                logger.warning(f"No jobs found on page {page_count + 1}")
            
            # Pagination logic
            if 'pageProps' in data and 'pageCursors' in data['pageProps']:
                page_cursors = data['pageProps'].get('pageCursors', {})
                
                # Debug pagination information
                logger.info(f"Page cursors info: {json.dumps(page_cursors)}")
                
                # Try different strategies to get the next cursor
                # Strategy 1: Get cursor for the next page
                current_page_num = page_count + 1
                next_cursor = page_cursors.get(str(current_page_num + 1))
                
                # Strategy 2: If that fails, try getting cursor based on total pages
                if not next_cursor:
                    next_cursor = page_cursors.get(str(len(page_cursors)))
                
                # Strategy 3: Look for "next" key if it exists
                if not next_cursor and "next" in page_cursors:
                    next_cursor = page_cursors.get("next")
                
                logger.info(f"Next cursor: {next_cursor}")
                
                if not next_cursor or next_cursor in visited_cursors:
                    logger.info("No more pages to scrape")
                    break
                
                visited_cursors.add(next_cursor)
            else:
                logger.info("No pagination information found")
                break
            
            page_count += 1
            
            # Add a small delay to avoid hitting rate limits
            time.sleep(2)
        
        logger.info(f"Scraped a total of {len(all_jobs)} jobs from {page_count + 1} pages")
        
        # Save to database
        if all_jobs:
            new_jobs_count = save_to_db(all_jobs)
            if new_jobs_count > 0:
                logger.info(f"Added {new_jobs_count} new jobs to the database")
                notify_success(new_jobs_count)
            else:
                logger.info("No new jobs added to the database")
                
        return all_jobs
    
    except Exception as e:
        error_message = f"Error in main scraper function: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "main_function")
        return []

if __name__ == "__main__":
    # Initialize database
    if init_db():
        get_company_list()
        scrape_simplyhired()
    else:
        logger.error("Failed to initialize database. Exiting.")