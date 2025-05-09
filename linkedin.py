import json
import time
import random
import os
import re
import pandas as pd
import requests
import uuid
import tempfile
import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
import logging
import html
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import create_engine, Column, Integer, String, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from rapidfuzz import process


TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []
company_name_map = {}
# Use a session for all requests
session = requests.Session()
# Add headers to mimic a real browser
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'cookie': 'bcookie="v=2&9c1ba741-33da-4490-8353-5e517c9fd006"; bscookie="v=1&20250420052037987ea26a-cc0f-4320-8738-bab240e7ed82AQE5aX3HpB52lDvEcJoSrxW3Ms69-rWj"; g_state={"i_l":0}; liap=true; JSESSIONID="ajax:8674298354585969947"; timezone=Asia/Calcutta; li_theme=light; li_theme_set=app; li_sugr=ee0ebf58-1d14-4d75-aa66-8efde35e8705; _guid=fe7e0ed1-05c0-4b73-b650-533efb753ce5; dfpfpt=5226265fe928420987c82aace8f33c11; aam_uuid=15281811367215937931823568750685877660; _gcl_au=1.1.1725562269.1745126453.1634867191.1745138867.1745138867; AnalyticsSyncHistory=AQIulSiDQDy4AgAAAZat94p5DJKR_mR6FycLlQrlCYqsOnTNsqMjnk4BiP3M9hxDh8yYbRA06EjtfZX5R-w6ew; lms_ads=AQG5sQftbUvZRAAAAZat94xp8KGTY7Zm9PlkX4cQV6ZL350sHgYXIuAWdV3szOoTGIBWHk2giiZygASYRvx_Ekw6vbzVVdwt; lms_analytics=AQG5sQftbUvZRAAAAZat94xp8KGTY7Zm9PlkX4cQV6ZL350sHgYXIuAWdV3szOoTGIBWHk2giiZygASYRvx_Ekw6vbzVVdwt; li_at=AQEDASx0kQIAfLf8AAABllGkWS0AAAGW0gQQRU4AWEDKXMnZca3sVxEGK7upbWx8fEAYlw8x9cXdyDf3yEbcInmKJaqL1paheVCFLXeGndLbr401GEbqEu9Sql5r7KhtQCUN_bjL2EWesfXVhfXRvIyY; lang=v=2&lang=en-us; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C20219%7CMCMID%7C15836129612716252261837944879390426711%7CMCAAMLH-1747488102%7C12%7CMCAAMB-1747488102%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1746890502s%7CNONE%7CMCCIDH%7C906603343%7CvVersion%7C5.1.1; fptctx2=taBcrIH61PuCVH7eNCyH0B9zcK90d%252bIeoo1r5v7Zc25F352gmAMPQk0PxTB%252f2M7huaYqAZXls1dbLHadNpQvr6WcyIqsQnbTi9obL%252bLsH1LHy%252fDbDQKyATX%252fVGsvhAEgqYK70ezsmjSCaG3AEHJVp2HBdFXyx04ZEb542OPp2EBFzV4iUiQhTY9c4rFchQ5sVWGNmqL1ahzDoVK0MZmb7IBxZFrQqCN%252f0niONWZvgowolnLVxQ0vaT7VwnDjqFY%252bNcl64pPzBHYnaFjqH1Xt9gOSEfwrLvti8XBqCvXOo8V2IKc%252bpQHA4BPoRA6EfaKk40iGSvNaN62MHcZ0nbFaWV3a4OuDlrk9IICO7L4u9G0%253d; UserMatchHistory=AQIqM82_9_1PsAAAAZa6YrSkcmRISyOrlKNFpBNW7Q7kFZM5b_NoDUS5AA9V5oh7yxIHPuzKIIO84_HXh5Hb_WGAUzYGpcoi5Q76dmryqo6dyEh-kVOiq8d8R265Kwm4ir4eNt97HaXZHvDvVM_cKvWMiUMoz4UYT7ZvqxUpGVZp7rRb6qDl8B0XJUHJ1i_QXSk4pRhavp40JsAoqE9CgF07rdPXwbV7pAWk0pe9nH-mxA6S53YUXPmOxSKc8U5RIVEKdWQUIthAp5n_dfVgjFUE6PwZ11C37n8oFhlE7BB4VMncpOD8qqMf9MdpgSOpYKmL6QG0csG76md7fbf6B5qTJHBBQtkNd5w0EnY1RPaaWyuW9Q; lidc="b=OB02:s=O:r=O:a=O:p=O:g=4682:u=2451:x=1:i=1746883754:t=1746894920:v=2:sig=AQE3GJN1sujOea79fIa9WjrCyT3Fmdgq"',
})

# Create temp directory only once
temp_dir = os.path.join(tempfile.gettempdir(), f"playwright_{uuid.uuid4().hex}")
os.makedirs(temp_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database setup
Base = declarative_base()
engine = create_engine('postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres')
Session = sessionmaker(bind=engine)


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


def get_chat_id(TOKEN: str) -> str:
    """Fetches the chat ID from the latest Telegram bot update."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = session.get(url)
        info = response.json()
        chat_id = info['result'][0]['message']['chat']['id']
        return chat_id
    except Exception as e:
        print(f"Failed to get chat ID: {e}")
        return None


def send_message(TOKEN: str, message: str, chat_id: str):
    """Sends a message using Telegram bot."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        response = session.get(url)
        if response.status_code == 200:
            print("Sent message successfully")
        else:
            print(f"Message not sent. Status code: {response.status_code}")
    except Exception as e:
        print(f"Failed to send message: {e}")


def notify_failure(error_message, location="Unknown"):
    """Sends a failure notification to Telegram."""
    global chat_id, TOKEN
    try:
        if chat_id is None:
            chat_id = get_chat_id(TOKEN)
        if chat_id:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"❌ LINKEDIN SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
            send_message(TOKEN, message, chat_id)
    except Exception as e:
        print(f"Failed to send failure notification: {e}")


def clean_name(name):
    """Removes special characters from a company name, keeps the rest the same."""
    try:
        if not name:
            return ""
        name = str(name).strip()
        # Remove special characters but keep letters, numbers, and spaces
        name = re.sub(r'[^A-Za-z0-9\s]', '', name)
        return name.strip()
    except Exception as e:
        error_message = f"Failed to clean name: {str(e)}"
        notify_failure(error_message, "clean_name")
        return ""


def get_company_list():
    """Loads and cleans the list of target companies from CSV."""
    global company_list, company_name_map
    try:
        df = pd.read_csv(r"data/2025-04-04_-_Worker_and_Temporary_Worker.csv")
        # Pre-compute cleaned names once
        df['Clean Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Clean Name'])
        
        # Create a mapping from raw names to cleaned names for reference
        company_name_map = dict(zip(df['Clean Name'], df['Organisation Name']))
            
        print(f"Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise


def remove_duplicates(jobs_list):
    """Removes duplicate job listings based on job URL."""
    try:
        # Use a dictionary for faster duplicate checking
        unique_jobs = {}
        
        for job in jobs_list:
            # Use job URL as unique identifier
            job_url = job.get('job_url')
            
            if not job_url:
                # If no URL (shouldn't happen), use combo of title and company
                job_identifier = f"{job.get('title', '')}_{job.get('company', '')}_{job.get('location', '')}"
            else:
                job_identifier = job_url
                
            # Only keep the first occurrence of each unique job
            if job_identifier not in unique_jobs:
                unique_jobs[job_identifier] = job
        
        print(f"Removed {len(jobs_list) - len(unique_jobs)} duplicate jobs")
        return list(unique_jobs.values())
    except Exception as e:
        error_message = f"Failed to remove duplicates: {str(e)}"
        notify_failure(error_message, "remove_duplicates")
        return jobs_list  # Return original list if deduplication fails


# Helper function for human-like waiting
async def human_delay(min_seconds=1, max_seconds=3):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def extract_job_description(html_content):
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", type="application/ld+json")
        
        if not script_tag:
            return None

        job_json = json.loads(script_tag.string)
        raw_description = job_json.get("description", "")
        decoded_description = html.unescape(raw_description)
        return decoded_description
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error extracting job description: {e}")
        return None


def get_external_url_and_description(url):
    """Use regular requests instead of rnet to get the external URL and description"""
    try:
        # Add jitter to avoid rate limiting
        time.sleep(random.uniform(1, 3))
        
        response = session.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Failed to get page: {response.status_code}")
            return url, None
            
        content = response.text
        soup = BeautifulSoup(content, "html.parser")
        description = extract_job_description(content)

        
        external_url = url
        try:
            url_element = soup.find('code', id='applyUrl')
            if url_element:
                external_url = url_element.string
        except Exception as e:
            print(f"Error finding external URL: {e}")
            
        return external_url, description
    except Exception as e:
        print(f"Error in get_external_url_and_description: {e}")
        return url, None


# Pre-compute fuzzy matching threshold to avoid recomputation
def precompute_fuzzy_matcher():
    """Create a function that efficiently checks if a company name matches our target list"""
    if not company_list:
        return lambda name: (True, 100)
    
    def matcher(name):
        try:
            clean_company_name = clean_name(name)
            if not clean_company_name:
                return (False, 0)
                
            match, score, _ = process.extractOne(clean_company_name, company_list)
            return (score > 70, score)
        except Exception:
            return (False, 0)
            
    return matcher


def extract_job_details(html, company_matcher):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        job_listings = []
        
        # Debug counter for tracking matching results
        total_jobs = 0
        matching_jobs = 0

        for job in soup.select('ul.jobs-search__results-list li'):
            total_jobs += 1
            job_data = {}

            title_tag = job.select_one('h3.base-search-card__title')
            job_data['title'] = title_tag.get_text(strip=True) if title_tag else None

            company_tag = job.select_one('h4.base-search-card__subtitle a')
            company_name = company_tag.get_text(strip=True) if company_tag else None
            job_data['company'] = company_name
            
            # Use precomputed matcher
            is_match, score = company_matcher(company_name)
            
            salary_tag = job.select_one('ul.job-card-container__metadata-wrapper span[dir="ltr"]')
            salary = salary_tag.get_text(strip=True) if salary_tag else None

            # Include job if it matches our company filter and salary filter
            if is_match and (not salary or str(salary).lower() not in ['hour', 'day', 'hourly']):
                matching_jobs += 1

                location_tag = job.select_one('span.job-search-card__location')
                job_data['location'] = location_tag.get_text(strip=True) if location_tag else None

                time_tag = job.select_one('time')
                job_data['posted_time'] = time_tag.get_text(strip=True) if time_tag else None
                job_data['posted_datetime'] = time_tag.get('datetime') if time_tag else None

                link_tag = job.select_one('a.base-card__full-link')
                url = link_tag.get('href') if link_tag else None

                logo_tag = job.select_one(".artdeco-entity-image")
                job_data['logo_url'] = logo_tag.get('src') if logo_tag else None

                job_data['salary'] = salary
                job_data['job_url'] = url
                job_data['description'] = None
                job_listings.append(job_data)

        print(f"📊 Extraction stats: {matching_jobs}/{total_jobs} jobs matched target companies")
        return job_listings
    except Exception as e:
        error_message = f"Failed to extract job details: {str(e)}"
        notify_failure(error_message, "extract_job_details")
        return []


async def process_job_urls(job_listings, max_concurrent=5):
    """Process job URLs in batches with throttling to avoid rate-limiting"""
    if not job_listings:
        return job_listings
        
    # Function to process a batch of jobs
    async def process_batch(batch):
        tasks = []
        for job in batch:
            if not job.get('job_url'):
                continue
                
            # Create a task for this URL
            task = asyncio.create_task(
                asyncio.to_thread(
                    get_external_url_and_description,
                    job['job_url']
                )
            )
            tasks.append((job, task))
            
        # Wait for all tasks in this batch to complete
        for job, task in tasks:
            try:
                external_url, description = await task
                job['job_url'] = external_url.replace('"', '') if external_url else job['job_url']
                job['description'] = description
            except Exception as e:
                print(f"Error processing job URL: {e}")
                
        return batch
        
    # Process jobs in batches to limit concurrency
    updated_jobs = []
    batch_size = max_concurrent
    
    for i in range(0, len(job_listings), batch_size):
        batch = job_listings[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(job_listings) + batch_size - 1)//batch_size} with {len(batch)} jobs")
        processed_batch = await process_batch(batch)
        updated_jobs.extend(processed_batch)
        
        # Add a small delay between batches to avoid rate limiting
        if i + batch_size < len(job_listings):
            await human_delay(1, 2)
    
    return updated_jobs


def insert_jobs_to_db(job_listings):
    """Efficiently insert new jobs without deleting all existing records"""
    if not job_listings:
        logger.info("No jobs to insert into database")
        return 0
    
    inserted_count = 0
    
    try:
        session = Session()
        try:
            # Get existing LinkedIn job URLs to avoid duplicates
            existing_jobs = session.query(Job.apply_link).filter(Job.data_source == 'linkedin').all()
            existing_urls = {job[0] for job in existing_jobs}
            
            # Create a batch of jobs to insert
            jobs_to_insert = []
            for job in job_listings:
                job_url = job.get('job_url', '')
                
                # Skip if this URL already exists in the database
                if job_url in existing_urls:
                    continue
                    
                # Create new job object
                new_job = Job(
                    job_title=job.get('title', ''),
                    company_name=job.get('company', ''),
                    company_logo=job.get('logo_url', ''),
                    salary=job.get('salary', '') if job.get('salary') else None,
                    posted_date=job.get('posted_time', ''),
                    experience='Full-time',  # Default or extract from job data if available
                    location=job.get('location', '') if job.get('location') else None,
                    apply_link=job_url,
                    description=job.get('description', ''),
                    data_source='linkedin'
                )
                
                jobs_to_insert.append(new_job)
                inserted_count += 1
            
            # Bulk insert all jobs at once
            if jobs_to_insert:
                session.bulk_save_objects(jobs_to_insert)
                session.commit()
                logger.info(f"Updated database: Inserted {inserted_count} new jobs")
            else:
                logger.info("No new jobs to insert")
            
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


async def close_linkedin_popup(page):
    """Close LinkedIn sign-in popups using Playwright"""
    try:
        # List of potential selectors for dismiss buttons
        dismiss_selectors = [
            "button.modal__dismiss.contextual-sign-in-modal__modal-dismiss",
            "button.modal__dismiss[aria-label='Dismiss']",
            "button.contextual-sign-in-modal__modal-dismiss",
            ".modal__dismiss.contextual-sign-in-modal__modal-dismiss",
            "button[aria-label='Dismiss']"
        ]
        
        for selector in dismiss_selectors:
            try:
                # Check if the button exists and is visible
                button = await page.query_selector(selector)
                if button:
                    is_visible = await button.is_visible()
                    if is_visible:
                        print(f"🎯 Found specific dismiss button: {selector}")
                        await button.click()
                        print("✅ Closed LinkedIn sign-in popup")
                        await human_delay(1, 2)
                        return True
            except Exception:
                continue
        
        # If no button found with CSS selectors, try with JavaScript
        await page.evaluate("""
            // Try to find and hide any modal or overlay
            var modals = document.querySelectorAll('.modal, .modal__overlay, [role="dialog"], .artdeco-modal, .contextual-sign-in-modal');
            modals.forEach(function(modal) {
                if (modal && modal.style.display !== 'none') {
                    modal.style.display = 'none';
                    console.log('Hidden modal via JS');
                }
            });
            
            // Remove potential overlay backdrop
            var backdrops = document.querySelectorAll('.modal__overlay, .artdeco-modal-overlay');
            backdrops.forEach(function(backdrop) {
                if (backdrop) {
                    backdrop.remove();
                    console.log('Removed backdrop via JS');
                }
            });
            
            // Remove body classes that might disable scrolling
            document.body.classList.remove('overflow-hidden');
        """)
        print("🔧 Attempted JavaScript modal removal")
        
        return False
    except Exception as e:
        print(f"⚠️ Error handling LinkedIn popup: {e}")
        return False


async def close_popups(page):
    """Close various popups that might appear on LinkedIn"""
    try:
        # First try to close the LinkedIn signup modal
        await close_linkedin_popup(page)
        
        # Handle other types of popups
        popup_selectors = [
            '.artdeco-toasts_toasts',
            '.artdeco-toast-item__dismiss',
            '.msg-overlay-bubble-header__controls button',
            '.consent-page button',
            'button:has-text("Dismiss")',
            'button:has-text("Not now")',
            'button:has-text("No, thanks")'
        ]
        
        for selector in popup_selectors:
            try:
                elements = await page.query_selector_all(selector)
                for element in elements:
                    if await element.is_visible():
                        await element.click()
                        print(f"❌ Closed popup: {selector}")
                        await human_delay(0.5, 1)
            except Exception:
                continue
                
        # Use JavaScript as fallback for toasts
        await page.evaluate("""
            var toasts = document.querySelector('.artdeco-toasts_toasts');
            if (toasts) toasts.style.display='none';
            
            var overlays = document.querySelectorAll('.artdeco-modal');
            overlays.forEach(function(overlay) {
                if (overlay.style.display !== 'none') overlay.style.display='none';
            });
        """)
    except Exception as e:
        error_message = f"Error handling popups: {str(e)}"
        print(error_message)


async def check_page_content_updated(page, previous_job_count):
    """Check if page content has been updated after clicking 'See more'."""
    try:
        # Count current visible job cards
        current_job_cards = await page.query_selector_all(
            ".job-card-container--clickable, .jobs-search__results-list li")
        current_count = len(current_job_cards)
        
        # Check if we have more jobs than before
        if current_count > previous_job_count:
            print(f"✅ Page updated: {previous_job_count} → {current_count} jobs")
            return True, current_count
        else:
            print(f"❌ Page did not update: Still showing {current_count} jobs")
            return False, current_count
    except Exception as e:
        print(f"⚠️ Error checking page content: {e}")
        return False, previous_job_count


async def load_all_jobs(max_pages=15):
    """Load jobs with a maximum page limit to prevent endless scraping"""
    global chat_id
    
    async with async_playwright() as p:
        # Launch browser with stealth mode
        browser = await p.chromium.launch(
            headless=True,
        )
        
        # Create a browser context with specific options to avoid detection
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='Europe/London',
        )
        
        # Apply evasion script to avoid detection
        await context.add_init_script("""
            // Overwrite the navigator properties
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
        """)
        
        page = await context.new_page()
        
        try:
            jobs_found = 0
            consecutive_no_button_found = 0
            consecutive_page_not_updated = 0
            max_no_button_attempts = 3
            max_no_update_attempts = 2
            
            total_jobs = []
            processed_page_count = 0

            company_matcher = precompute_fuzzy_matcher()
            
            # LinkedIn jobs search URL
            URL = "https://www.linkedin.com/jobs/search/?currentJobId=4218290146&f_E=4&f_JT=F&f_SB2=42&f_TPR=r2592000&f_WT=1%2C3&geoId=101165590&keywords=&location=United%20Kingdom&origin=JOB_SEARCH_PAGE_JOB_FILTER"
            
            # Navigate to the page
            await page.goto(URL)
            await human_delay(4, 7)
            
            # Wait for job results to load
            try:
                await page.wait_for_selector(".jobs-search__results-list", timeout=10000)
                print("✅ Page loaded successfully")
            except PlaywrightTimeoutError:
                print("⚠️ Page took too long to load, but continuing anyway")
            
            # Close any initial popups
            await close_popups(page)
            
            # Main loop - with max page limit for safety
            while processed_page_count < max_pages:
                await close_popups(page)
                
                # Scroll smoothly for more human-like behavior
                scroll_height = await page.evaluate("document.body.scrollHeight")
                for i in range(0, scroll_height, random.randint(300, 700)):
                    await page.evaluate(f"window.scrollTo(0, {i})")
                    await human_delay(0.1, 0.3)
                
                # Count visible job cards to track progress
                job_cards = await page.query_selector_all(
                    ".job-card-container--clickable, .jobs-search__results-list li")
                current_job_count = len(job_cards)
                
                # Get page content and extract jobs
                page_content = await page.content()
                jobs = extract_job_details(page_content, company_matcher)
                
                # Add new jobs to our list
                total_jobs.extend(jobs)

                processed_page_count += 1
                
                # Every few pages, deduplicate and save results
                if processed_page_count % 3 == 0:
                    # Remove duplicates before saving
                    total_jobs = remove_duplicates(total_jobs)
                    
                    with open("linkedin_filtered_jobs.json", "w", encoding="utf-8") as json_file:
                        json.dump(total_jobs, json_file, indent=2, ensure_ascii=False)
                    print(f"💾 Saved {len(total_jobs)} unique jobs to JSON (after {processed_page_count} pages)")
                
                # Print progress only if we found more jobs
                if current_job_count > jobs_found:
                    print(f"📊 Found {current_job_count} total jobs, {len(total_jobs)} matching companies")
                    jobs_found = current_job_count
                
                # Try to find and click "See more" button
                button_found = False
                
                try:
                    # Try different approaches to find the "See more" button
                    see_more_selectors = [
                        "button.infinite-scroller__show-more",
                        "button:has-text('See more')",
                        "button:has-text('Show more')",
                        ".infinite-scroller__show-more-button",
                        ".more-jobs-button"
                    ]
                    
                    see_more_button = None
                    for selector in see_more_selectors:
                        button = await page.query_selector(selector)
                        if button and await button.is_visible():
                            see_more_button = button
                            button_found = True
                            break
                    
                    if see_more_button:
                        # Make sure the button is in view
                        await see_more_button.scroll_into_view_if_needed()
                        await human_delay(1, 2)
                        
                        # Remember job count before clicking
                        pre_click_job_count = current_job_count
                        
                        # Click the button
                        await see_more_button.click()
                        print("🔄 Loading more jobs...")
                        await human_delay(3, 5)  # Give more time to load
                        
                        # Check if the page has actually been updated with new content
                        page_updated, new_job_count = await check_page_content_updated(page, pre_click_job_count)
                        
                        if page_updated:
                            consecutive_no_button_found = 0  # Reset button counter on success
                            consecutive_page_not_updated = 0  # Reset update counter on success
                        else:
                            consecutive_page_not_updated += 1
                            print(f"⚠️ Page didn't update after clicking 'See more' ({consecutive_page_not_updated}/{max_no_update_attempts})")
                            
                            # If we've had too many consecutive non-updates, assume we're done
                            if consecutive_page_not_updated >= max_no_update_attempts:
                                print("⛔ Too many failed page updates. Breaking loop.")
                                break
                    else:
                        consecutive_no_button_found += 1
                        print(f"⚠️ No 'See more' button found (attempt {consecutive_no_button_found}/{max_no_button_attempts})")
                        await human_delay(2, 3)  # Wait a bit and try again with a fresh scroll
                        
                except Exception as e:
                    print(f"⚠️ Click attempt failed: {e}")
                    consecutive_no_button_found += 1
                    await human_delay(1, 2)
                
                # If we haven't found the button for several consecutive attempts, assume we've reached the end
                if consecutive_no_button_found >= max_no_button_attempts:
                    print(f"✅ No more 'See more' buttons found after {max_no_button_attempts} attempts. Done loading jobs.")
                    break
            
            # Final job count
            final_count = len(await page.query_selector_all(
                ".job-card-container--clickable, .jobs-search__results-list li"))
            
            # Final deduplication
            total_jobs = remove_duplicates(total_jobs)
            
            # Process job URLs in batches - this is now done after all scraping to avoid rate-limiting during page navigation
            print("Processing job URLs to get external links and descriptions...")
            total_jobs = await process_job_urls(total_jobs, max_concurrent=5)
            
            # Save to database
            insert_jobs_to_db(total_jobs)
            
            print(f"🏁 Total jobs loaded: {final_count}")
            print(f"🏁 Filtered jobs (matching companies): {len(total_jobs)}")
            
            # Send success notification
            if chat_id is None:
                chat_id = get_chat_id(TOKEN)
            if chat_id:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                message = f"✅ LINKEDIN SCRAPER SUCCESS at {timestamp}\nTotal jobs loaded: {final_count}\nMatching companies: {len(total_jobs)}"
                send_message(TOKEN, message, chat_id)
            
            return len(total_jobs)
            
        except Exception as e:
            error_message = f"Error in load_all_jobs: {str(e)}"
            notify_failure(error_message, "load_all_jobs")
            return 0
        finally:
            # Always close the browser
            await browser.close()
            print("Browser closed")


# Entry point of the script
async def main():
    try:
        # Initialize chat_id early to avoid scope issues
        global chat_id
        if chat_id is None:
            chat_id = get_chat_id(TOKEN)

        # Setup database
        setup_database()
        
        # Load companies list
        get_company_list()
        
        # Start the scraping process
        await load_all_jobs()
    except Exception as e:
        error_message = f"Critical failure in main execution: {str(e)}"
        notify_failure(error_message, "main_execution")


if __name__ == "__main__":
    asyncio.run(main())