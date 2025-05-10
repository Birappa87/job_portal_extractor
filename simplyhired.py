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
            if score > 80 and not any(unit in salary.lower() for unit in ['hour', 'day', 'hourly']):
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
        'gdId': 'a9a3794f-c3cd-4dc3-ba7a-bd24bb8cd9a2',
        'indeedCtk': '1ipgg2g8jk8ii801',
        'rl_page_init_referrer': 'RudderEncrypt%3AU2FsdGVkX18YwXiSsJngik2uKCMUD3VqwUx1sp%2BvgH4%3D',
        'rl_page_init_referring_domain': 'RudderEncrypt%3AU2FsdGVkX18mTosiyE8JBUueuG8L01kt516kMhkqzq8%3D',
        '_optionalConsent': 'true',
        'ki_s': '240196%3A0.0.0.0.0',
        '_gcl_au': '1.1.1933671595.1745643872',
        '_fbp': 'fb.2.1745643872476.193155387393605754',
        'trs': 'INVALID:SEO:SEO:2021-11-29+09%3A00%3A16.8:undefined:undefined',
        'uc': '8013A8318C98C5172ACA70CF4222A8AAD282B8714CD4AADAA8AB8B9B95BD6A3D51F4CEEA8A10DE766A74B2DA5561A91E679EDE37C16A7B1F66AF1F6FCDE359C48973A874B4F8E8FD3CFFC10792B9AEBA6A2C27B929C93FBB029090B32FA6A89ACDC1996A94C9A1B36C00CA3CEA6A05EE2A2131D99E0789229F4BA87DAC72A45A4CC89960A391E37037E1A04EEDF5BDCD',
        'ki_r': 'aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS8%3D',
        'asst': '1746679780.2',
        'alr': 'https%3A%2F%2Fwww.google.com%2F',
        'rsSessionId': '1746679778767',
        'JSESSIONID': 'E1C25E02319F356ED3B989A462B3BA41',
        'GSESSIONID': 'E1C25E02319F356ED3B989A462B3BA41',
        'cass': '0',
        'cdzNum': '3',
        'AWSALB': 'IEg334ns0dPRIj5oSkJCoDistgqU1xihXlu6dL9ve/BIm/OPLAz7BOhHwkS0ZrrUNTrWbTRxWeyvCOazGBPNTHz0rhdFCE4Q6YUOyKsBEp76ufmRuuUzzhXYG++Q',
        'AWSALBCORS': 'IEg334ns0dPRIj5oSkJCoDistgqU1xihXlu6dL9ve/BIm/OPLAz7BOhHwkS0ZrrUNTrWbTRxWeyvCOazGBPNTHz0rhdFCE4Q6YUOyKsBEp76ufmRuuUzzhXYG++Q',
        'at': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3NWFkZDk2Ny0yZGM3LTQwYzMtYTY1My0zODllNzBiMWEzNzUiLCJ1IjoiYmlyYXBwYS4wMDFAZ21haWwuY29tIiwidWlkIjoyMTkwNTMyMTcsInJmc2giOjE3NDY2ODE1NDMxNjUsInJtYl9pYXQiOjE3NDU2NDQwNDIyNjksInJtYl9leHAiOjE3NzcxODAwNDIyNjksImF0dCI6InJtIiwiYXV0aG9yaXRpZXMiOls0NSw0NywyOF19.s9oSM125-0KNfi78FuERfncfeLO48Qpp_BdZT99fXEBqvrZtKn1_l2zM2wefu_IZkiBpqyAPxLfG0V5RxpKToZSf30hVScSqNPBCRj636pELXyjxrHGFKqwDY0aH4UbxOCeQwwvqG27toyVrcjKLuvPwlnAGdQ4NguVR51Lx02YzFrarY2KItN6_KpDTfuJIJHOBeYCfU58_XWVjJ120mdYMfRXZBzBBZ-Sn5HWSzuZLTtT8jbby5jmZ9iUK4fuiOjAFNPxBdTgP15r11_dtRHnA4sLBwRLiygoRoLwS6e62m7teX9GMH8-vy2WLZVOqmGB-2TN-8SnqsAH_TgDrbw',
        '__cf_bm': '2u3yeGXcZqQMkp3yFgTGLRqT6gbRRK9JyzOFkAWZ5XA-1746680943-1.0.1.1-0y3cM7L8klJVvuD_27gpGNHluDqTUR2Mv3aHO7HI6tbQpgJPrCjPKz.w5NFJsaDrlE__oXoZgZW3WALrmXjIuIVkoq4Ko0QK90y3wcYyPeI',
        '_cfuvid': 'F1JyP4rTk8yDoXsNq0Fax0xMyleuyIaP4.7c6xox624-1746680943399-0.0.1.1-604800000',
        'gdsid': '1746679780949:1746680954685:18002F6900B5EF7C47D538DE0558DB53',
        'bs': 'fv-Crz8uGWx2pdHZlyqM4Q:DXD1LUUEvXIKyahs7xoi3NRFVGEV4lfb9LBuK5H_131hjEsw8iILMX9l2tnEtTUDAfpFqylfPoDCx5MgJ-oBWsHZ3PJg7A-gNLVS4jbU2es:pv3Qyn22VUBOh7JUdHkNb-SpokNsPzoobX-bLROYopA',
        'rl_user_id': 'RudderEncrypt%3AU2FsdGVkX1%2Bc1JduU1AXZlvaL%2FzCSG1uhfTquXntCq8%3D',
        'rl_trait': 'RudderEncrypt%3AU2FsdGVkX1%2FpDAq%2FGAyCnJy2h5GFgtn53kp8M0CcN6fr%2BZv0WLB70ytgWEK785PU%2BHdfmVR6cTmIcQHn18HWPabRoDTl7ka7pnj5dNOi23Pawj03FqJXpJ%2BwsQrlwAhlJgOzNfpLg4RaA5jISi2mlVSZajK8nsXR1SVR3ERG6ho%3D',
        'rl_group_id': 'RudderEncrypt%3AU2FsdGVkX18IhDVDy%2BvAGPyB%2FbAt8f0c2LVuLNzD7WM%3D',
        'rl_group_trait': 'RudderEncrypt%3AU2FsdGVkX18Z3ZBcl7k29yJnC7LO6yJCUhArw1ITQh4%3D',
        'OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+May+08+2025+10%3A39%3A15+GMT%2B0530+(India+Standard+Time)&version=202407.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1eafc5e2-9db4-4939-85fe-8f9a22faffeb&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0017%3A1&AwaitingReconsent=false',
        'rl_anonymous_id': 'RudderEncrypt%3AU2FsdGVkX18%2F0wTVQ3hF5SVuCwCDfMf7dpBJpSkLVPbp50D7mCtkSbwax9W6ad7pkqnosJSA08V1gvKOTnkyGA%3D%3D',
        'rsReferrerData': '%7B%22currentPageRollup%22%3A%22%2Fjob%2Fjobs-srch%22%2C%22previousPageRollup%22%3A%22%2Fjob%2Fjobs-srch%22%2C%22currentPageAbstract%22%3A%22%2FJob%2F%5BOCC%5D-jobs-SRCH_%5BPRM%5D.htm%22%2C%22previousPageAbstract%22%3A%22%2FJob%2F%5BOCC%5D-jobs-SRCH_%5BPRM%5D.htm%22%2C%22currentPageFull%22%3A%22https%3A%2F%2Fwww.glassdoor.co.uk%2FJob%2Funited-kingdom-barclays-jobs-SRCH_IL.0%2C14_IN2_KO15%2C23.htm%22%2C%22previousPageFull%22%3A%22https%3A%2F%2Fwww.glassdoor.co.uk%2FJob%2Funited-kingdom-barclays-jobs-SRCH_IL.0%2C14_IN2_KO15%2C23.htm%22%7D',
        'rl_session': 'RudderEncrypt%3AU2FsdGVkX1%2FBmQoXNtg7dY0v5cqLkiXNYuciUZA8Rg0sVdUTRBzJ%2F2eqApofeRsGtJCNHIKwn38zra0H7LlvAwZ2IHJYnu02m35lo%2BNuzf%2FhSLheUgg9qj2oEaa2f2ZrI7ViQw%2BT%2B2qfJFV62tm8Kw%3D%3D',
        'ki_t': '1745384200319%3B1746680958290%3B1746680958290%3B5%3B44',
        '_dd_s': 'rum=0&expire=1746681892273',
        'cdArr': '217',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'apollographql-client-name': 'job-search-next',
        'apollographql-client-version': '7.176.5',
        'content-type': 'application/json',
        'gd-csrf-token': 'T-u135XkZCctN-eQXFsChA:16tYnNJQvfUh3cmuhAfyEVmLkHhS3LLabwuSxwbySUJk8Z8lOrgRwW-XLt4_qovn84YVoO3gKst6rWf2nuEDnQ:AilBgkzIhrNjAm9y61oOyvtdNaWI61nKkdy2u3Fq1QM',
        'origin': 'https://www.glassdoor.co.uk',
        'priority': 'u=1, i',
        'referer': 'https://www.glassdoor.co.uk/',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'x-gd-job-page': 'serp',
    }

    params_base = {
        'l': 'United Kingdom',
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
