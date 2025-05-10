import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd
import re
import json
import time
import logging
import traceback
from sqlalchemy import create_engine, Column, Integer, String, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from rapidfuzz import process

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('totaljobs_scraper')

# Telegram Bot Setup
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []

# Database setup
Base = declarative_base()
engine = create_engine('postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres')
Session = sessionmaker(bind=engine)

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

# Function to ensure database tables exist
def setup_database():
    """Create database tables if they don't exist"""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        error_message = f"Database setup error: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "setup_database")

# Helper functions for database operations
def truncate_string(text: str, max_length: int) -> str:
    """Truncate string to specified maximum length"""
    return text[:max_length] if text else ""

def parse_date(date_str: str) -> datetime.date:
    """Parse date string into a datetime.date object"""
    try:
        # Handle different date formats from TotalJobs
        if 'today' in date_str.lower():
            return datetime.now().date()
        elif 'yesterday' in date_str.lower():
            return (datetime.now() - timedelta(days=1)).date()
        
        # Try to parse other date formats
        date_patterns = [
            r'(\d+)\s+days?\s+ago',  # "3 days ago"
            r'(\d+)\s+weeks?\s+ago'  # "2 weeks ago"
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str.lower())
            if match:
                time_value = int(match.group(1))
                if 'day' in date_str.lower():
                    return (datetime.now() - timedelta(days=time_value)).date()
                elif 'week' in date_str.lower():
                    return (datetime.now() - timedelta(weeks=time_value)).date()
        
        # Try standard date formats
        for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d %b %Y']:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
                
        # If all else fails, return today's date
        logger.warning(f"Could not parse date: {date_str}, using current date")
        return datetime.now().date()
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {str(e)}")
        return datetime.now().date()

# Telegram functions
def get_chat_id(TOKEN: str) -> str:
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = requests.get(url)
        info = response.json()
        return info['result'][0]['message']['chat']['id']
    except Exception as e:
        logger.error(f"Failed to get chat ID: {e}")
        return None

def send_message(TOKEN: str, message: str, chat_id: str):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {'chat_id': chat_id, 'text': message}
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logger.info("✅ Telegram message sent.")
        else:
            logger.warning(f"⚠️ Telegram error: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def notify_failure(error_message, location="Unknown"):
    global chat_id
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ TOTALJOBS SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(message):
    global chat_id
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        send_message(TOKEN, message, chat_id)

# Cleaning and matching
def clean_name(name):
    try:
        name = str(name).strip().lower()
        name = re.sub(r'[^a-z0-9\s]', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name
    except Exception as e:
        notify_failure(f"Failed to clean name: {str(e)}", "clean_name")
        return ""

def get_company_list():
    global company_list
    try:
        df = pd.read_csv(r"data/2025-04-04_-_Worker_and_Temporary_Worker.csv")
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Organisation Name'])

        logger.info(f"✅ Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        notify_failure(f"Failed to load company list: {str(e)}", "get_company_list")
        raise

# Scraper logic
def fetch_totaljobs_page(url: str) -> str:
    """Fetch HTML content from the specified URL"""
    cookies = {
        'anonymousUserId': '54bafa4b-7ecd-9bec-fb09-e4fb65b60c74',
        'VISITOR_ID': 'c2be550f9940928eb550d627cecd00f1',
        'bm_sz': '02D148594D97D3ACCCC3B4374A95B63D~YAAQdAHARTMc50GWAQAAoCWJYxvq/H9NLib74uNYZcvStV+odCIE2tZQHn3zb/OL2+pV76kHZulTLXQmGfZxDAXiXYmGnu1nYbgI3+1YS5gjPefJktcATixyCsI+Cr3Ay5RR7w54YlegHnBTAECBuHlM40Ij0dwLKgnnrtPfg2jo2nYT0GUlVqSHUEe9dZO/rLvKhJg9YyIujrQhfO4Mn9zq7LtCWB7O+KC/LMNesC8JKozietZqsZ2SKA402aOg95pG96H4y6WgbjYlSmz32calZKZk/I5RknlCK0I1QbNf5qS1gMnGoqheAcx60f7opcr12x8nRUh4aLyn6STk3OWPNkqwoPkDdzTAi82zl1jc0FsGHFXym2TI1nK+lYE5XYgeJrah0m7exDchEnFi~4403521~4535863',
        'optimizelySession': '1745426651660',
        'RT': '"z=1&dm=www.totaljobs.com&si=dbcd41f2-e862-4d01-8d0e-e86eeddee462&ss=m9u5ymji&sl=0&tt=0"',
        'sc_vid': '85365f049a602a427e99365c84a07324',
        'listing_page__qualtrics': 'empty',
        'AnonymousUser': 'MemberId=759c4cbe-209d-4791-ad59-c1d862a3f67b&IsAnonymous=True',
        'SessionCookie': 'a61638c8-4964-4325-85f1-43d3d9ea2222',
        'SsaSessionCookie': '08d6b32c-fdff-47a9-b13c-5d4d562f5138',
        '_abck': '345880C05C88AD64AFAE1B4AD75482B2~0~YAAQdAHARUAc50GWAQAAqy2JYw1EAByyeMCcMXQTSgiFnPwxkSXEupi19aP/6l/KeodOH8nur2GFYeBqZWZ2LqfD+5ABqePwYmTTVbIdjUKytBVqauOCukjMVsSHSlBtKg+MDDOCEczUUomHYpK9CqeuaLLQi6dJQngOVKUGyUbVm5oV8TPG8Sd1WpuanJSDO5FOsn492w1BL1cnVsSDY1su18G/KU/9rbbRxuan1D9Ph5+/wPlf88qd3qQBt6FjxuDaCc04CV4QSHH7hO0mZWVGi+EnTUe7Rayo3MXymo3FWso1DhUnYC7G4//01GvfSiXlvAyL1MSegeH0yIIHG/C3GtzwgIiqlIr0i3N6YHgKLkgJXaKA+yxoLC+mIcSOeaIj7wm4d5ndMmRGBhcRoiCW1mh2OyU3M1TCu3n+58roGQGlNhP64FGem1yGZi2N6oGbLAZdtxKSX+6ViaLjwU9IWdO8GtPJMzaKGZIFmoBsEszKbePpnJCxqJ0NY5h91VUFubXwznXB17AlY82RBo8MyfkdbLtON+/9xmlrng==~-1~||0||~-1',
        'EntryUrl': '/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1&',
        'SearchResults': '"104773239,104740055,104707469,104695324,104678831,104601614,104521678,104695119,104561633,104742194,104737809,104737808,104722229,104634780,104634782,104634772,104634775,104634770,104629792,104676579,104676604,104676621,104676660,104568931,104655837"',
        'CONSENTMGR': 'c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:0%7Cc6:1%7Cc7:1%7Cc8:0%7Cc9:1%7Cc10:0%7Cc11:0%7Cc12:1%7Cc13:1%7Cc14:0%7Cc15:0%7Cts:1745426654906%7Cconsent:true',
        'ak_bmsc': 'B3A79D6AF363ED8BC4F4413E298574F1~000000000000000000000000000000~YAAQdAHARUgc50GWAQAA+TSJYxtbqHyHp6YgLKTEI6k56aLIVPz+uBN6/lxAvEEcbnAR0nj0a9TIoC3+Xcp//xcPrslXfsnBNkExgEduQds0kFX0bGcbUEv17SWEb7LtPlcKlyPzGcOvj6rMDPJ1mGrGaJzD8wBpq5rJML5udxLybaJWXlwPikAmCOm2/6hyOTXoB4lwy3HJ24NMGTf0TAIpFip6i80D28vUBgxfFe4hIei2Ai+Lw0sunExrrn82clVESOtl2mJ3oOJBho12h1uer0vSuWqh0zAmSjFfVOb3ar2fvUAh91M2Nt2H1qcNGdA/2+g0DKiJx3e7Uy/ta8FgVDCowyRCI/enbMshUrWoQUbey/4qIunaU5OFAkcY5Wh+WJ6Un0izNjLqulEIKCoUjjfCdnGPAd2qf5LCq353Wxd11EvHN70ddMy8uYgvSeoxkSVuZAbIMSaLfON8',
        '_fbp': 'fb.1.1745426656102.552846486146374068',
        'utag_main': 'v_id:019663891adf000720adaf95a1e30506f005906700978$_sn:1$_se:4$_ss:0$_st:1745428455250$ses_id:1745426651874%3Bexp-session$_pn:1%3Bexp-session$PersistedFreshUserValue:0.1%3Bexp-session$prev_p:resultlist%3Bexp-session$dc_visit:1$dc_event:1%3Bexp-session$dc_region:eu-central-1%3Bexp-session',
        '_uetsid': '31c3a820206211f0a0db23d3adf41216',
        '_uetvid': 'b5fa0900188111f0af7a07bdcdc07c19',
        '_gcl_au': '1.1.1525417813.1745426656',
        '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22759c4cbe-209d-4791-ad59-c1d862a3f67b%22%2C%22expiryDate%22%3A%222026-04-23T16%3A44%3A16.501Z%22%7D',
        '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22pwU7WZAJpJ1yjKLsMh5z%22%2C%22expiryDate%22%3A%222026-04-23T16%3A44%3A16.503Z%22%7D',
        '_ga_F6MR9F9R5K': 'GS1.1.1745426651874.1.0.1745426656.0.0.0',
        '_ga': 'GA1.1.111026824.1745426657',
        '_clck': '1yzdi95%7C2%7Cfvb%7C0%7C1936',
        'cto_bundle': 'TqUO1l81Y3MlMkIlMkJwNElISjE0S0VtR1BsRUZtQ052V08zUCUyQjRFZVZ5UnRMV3JvdWElMkJCekQ4ZVE1eE0wNHVUVyUyQkNteVVWUW1EcXhlZER0ck9FT25hY1hVMCUyQkh6NzZmOXRtSmRHUyUyQlYxV2R4UTF0cHBUSTNzYkREM1NHazlwa1ZObGdYRSUyRnRvZm8yOVFtdUdBV2ZIN2QlMkJsTlpuVUxIalVQU0JadUhwRiUyRk9Ed3NVZUpCZHclMkZDS0xCSzBCMXNZWDhOa3pKMWdOVmNVMDF5Zms3VVB6TU43SmRvbU5jTXRSMTZ3c1pnQjBvMmVKWU9MVTZJQURXUUwlMkJYWDRLTW9rWEhxU25vbnZRJTJGVjdyU3RxJTJCYXpaemhPOE5DYyUyQjFLQSUzRCUzRA',
        '_hjSessionUser_2060791': 'eyJpZCI6ImFhNTg1NmE5LTEwMzAtNTU1Zi05MjhhLWRmZGEwY2I3Nzk5NCIsImNyZWF0ZWQiOjE3NDU0MjY2NTcyNTgsImV4aXN0aW5nIjp0cnVlfQ==',
        '_hjSession_2060791': 'eyJpZCI6IjhkZWE0NTkyLTc2ODQtNDIxZS1hNDViLWM1Yjk3MDJiZjQ1YyIsImMiOjE3NDU0MjY2NTcyNjEsInMiOjEsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=',
        '_dd_s': 'logs=1&id=734f9fa9-428a-4b49-96ef-dac4200ea061&created=1745426653411&expire=1745427555001',
        '_clsk': 'feqk90%7C1745426657731%7C1%7C1%7Cd.clarity.ms%2Fcollect',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7,kn;q=0.6',
        'priority': 'u=0, i',
        'referer': 'https://www.totaljobs.com/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1&action=facet_selected%3bsalary%3b30000%3bsalarytypeid%3b1',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36',
    }

    try:
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url, cookies=cookies, headers=headers)

        if response.status_code == 200:
            logger.info(f"Successfully fetched page from URL: {url}")
            return response.text
        else:
            raise Exception(f"Failed to fetch page from URL: {url}, status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error fetching page from URL {url}: {str(e)}")
        raise

def extract_job_data_and_pagination(html_content: str, current_page: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Extract job data from preloaded state in script tags and return pagination info
    Returns: (job_listings, next_page_url)
    """
    jobs = []
    next_page_url = None
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Pattern to find JSON assignment inside script
        pattern = r'window\.__PRELOADED_STATE__\["[^"]+"\]\s*=\s*({.*?});'
        
        # List to collect all extracted JSON objects
        extracted_data = []
        
        # Loop through all script tags
        found_data = False
        for script in soup.find_all('script'):
            if script.string and 'window.__PRELOADED_STATE__' in script.string:
                matches = re.finditer(pattern, script.string, re.DOTALL)
                for match in matches:
                    json_text = match.group(1)
                    try:
                        data = json.loads(json_text)
                        extracted_data.append(data)
                        found_data = True
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decoding error: {e}")
        
        if not found_data:
            logger.warning("No preloaded state data found in the HTML")
            return [], None
            
        # Process the extracted data (using the second JSON object which contains search results)
        if len(extracted_data) > 1 and 'searchResults' in extracted_data[1]:
            search_results = extracted_data[1]['searchResults']
            
            # Extract pagination info
            if 'pagination' in search_results:
                pagination = search_results['pagination']
                logger.info(f"Pagination info: Page {pagination.get('page')} of {pagination.get('pageCount')}, Total: {pagination.get('totalCount')}")
                
                # Get next page URL if available
                if 'links' in pagination and 'next' in pagination['links'] and pagination['links']['next']:
                    next_page_url = pagination['links']['next']
                    logger.info(f"Next page URL: {next_page_url}")
            
            # Extract job items
            job_items = search_results.get('items', [])
            base_url = "https://www.totaljobs.com"
            
            for job in job_items:
                # Clean and check company name
                raw_company = job.get('companyName', 'N/A')
                try:
                    match, score, _ = process.extractOne(raw_company, company_list)
                except Exception as e:
                    print(f"Error in fuzzy matching: {e}")
                    match, score = company_name, 0
                
                if score < 70:
                    continue
                
                salary = job.get('salary', 'N/A')
                if any(unit in salary for unit in ['per hour', 'hourly', 'an hour', 'a day', 'per day', '/hour', '/day']):
                    continue

                # Extract job data
                job_data = {
                    'job_id': job.get('id'),
                    'title': job.get('title', 'N/A'),
                    'company': raw_company,
                    'company_logo': job.get('companyLogoUrl', 'N/A'),
                    'salary': job.get('salary', 'N/A'),
                    'posted_date': job.get('datePosted', 'N/A'),
                    'location': job.get('location', 'N/A'),
                    'url': base_url + job.get('url', ''),
                    'description': job.get('textSnippet', 'N/A'),
                    'experience': job.get('jobType', ''),
                    'apply_link': base_url + job.get('url', ''),
                    'country': "UK",
                    'source': "totaljobs",
                    'page_number': current_page,
                    'ingestion_timestamp': datetime.utcnow().isoformat()
                }
                
                # Try to extract additional info
                job_data['labels'] = job.get('badges', [])
                if isinstance(job_data['labels'], list) and job_data['labels']:
                    job_data['experience'] += f" - {', '.join([b.get('text', '') for b in job_data['labels'] if 'text' in b])}"

                jobs.append(job_data)
                
            logger.info(f"Extracted {len(jobs)} matching jobs from preloaded state on page {current_page}")
        else:
            logger.warning(f"Could not find search results in extracted data on page {current_page}")
            
    except Exception as e:
        error_message = f"Error extracting job data: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        notify_failure(error_message, "extract_job_data_and_pagination")
        
    return jobs, next_page_url

# Database operations
def insert_jobs_to_db(job_listings: List[Dict[str, Any]]) -> int:
    """Delete all jobs with data_source='totaljobs' and insert new jobs"""
    if not job_listings:
        logger.info("No jobs to insert into database")
        return 0
    
    inserted_count = 0
    
    try:
        session = Session()
        try:
            # Delete all existing records with data_source='totaljobs'
            logger.info("Deleting all existing TotalJobs jobs from database...")
            deleted_count = session.query(Job).filter(Job.data_source == 'totaljobs').delete()
            logger.info(f"Deleted {deleted_count} existing TotalJobs jobs")
            
            # Insert new records
            logger.info(f"Inserting {len(job_listings)} new jobs...")
            for job in job_listings:
                try:
                    # Parse the posted date
                    posted_date = job.get('posted_date', '')
                    
                    # Truncate strings to fit database columns
                    job_title = truncate_string(job.get('title', ''), 255)
                    company_name = truncate_string(job.get('company', ''), 255)
                    salary = truncate_string(job.get('salary', ''), 100)
                    experience = truncate_string(job.get('experience', ''), 100)
                    location = truncate_string(job.get('location', ''), 255)
                    description = job.get('description', '')

                    if 'Visas cannot be sponsored' in description:
                        continue

                    # Create new Job object with all fields from schema
                    new_job = Job(
                        job_title=job_title,
                        company_name=company_name,
                        company_logo=job.get('company_logo', ''),
                        salary=salary,
                        posted_date=posted_date,
                        experience=experience,
                        location=location,
                        apply_link=job.get('apply_link', ''),
                        description=description,
                        data_source='totaljobs'
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

# Main function
if __name__ == "__main__":
    try:
        # Ensure database is set up
        setup_database()
        
        # Get company list 
        get_company_list()
        
        # Set maximum pages to scrape (as a safety measure)
        max_pages = 200
        
        # Set initial URL
        current_url = 'https://www.totaljobs.com/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1'
        current_page = 1
        all_jobs = []

        # Loop through pages using dynamic pagination
        while current_url and current_page <= max_pages:
            try:
                # Fetch the page
                html_content = fetch_totaljobs_page(current_url)
                
                # Extract job data and get next page URL
                job_listings, next_page_url = extract_job_data_and_pagination(html_content, current_page)

                if current_page == 2:
                    break

                # Add jobs to our collection
                if job_listings:
                    all_jobs.extend(job_listings)
                    logger.info(f"✅ Page {current_page}: {len(job_listings)} jobs matched and added. Total: {len(all_jobs)}")
                else:
                    logger.info(f"⚠️ Page {current_page}: No matching jobs.")
                
                # Save progress after each page
                with open("totaljobs_combined.json", "w", encoding="utf-8") as f:
                    json.dump(all_jobs, f, ensure_ascii=False, indent=4)
                
                # Set URL for next page
                current_url = next_page_url
                current_page += 1
                
                # If there's no next page URL, we're done
                if not current_url:
                    logger.info("Reached last page. Scraping complete.")
                    break
                    
                # Be nice to the server
                time.sleep(2)  

            except Exception as page_error:
                error_msg = f"Error processing page {current_page}: {str(page_error)}"
                logger.error(error_msg)
                notify_failure(error_msg, f"Page {current_page}")
                raise

        # After all pages are processed, insert jobs to database
        if all_jobs:
            inserted_count = insert_jobs_to_db(all_jobs)
            success_message = f"✅ TotalJobs Scraper completed successfully!\n📊 Stats:\n- Pages scraped: {current_page - 1}\n- Jobs matched: {len(all_jobs)}\n- Jobs inserted: {inserted_count}"
            logger.info(success_message)
            notify_success(success_message)
        else:
            notify_failure("No jobs found matching the criteria", "main")

    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        notify_failure("Script interrupted by user", "main")
    except Exception as e:
        error_message = f"Unexpected error in main function: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        notify_failure(error_message, "main")
    finally:
        logger.info("Script execution completed")