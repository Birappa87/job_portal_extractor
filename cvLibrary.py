import json
import asyncio
import re
import time
import threading
import pandas as pd
from datetime import datetime
import os
import traceback
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table, inspect, insert, select, update, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.expression import exists
from functools import lru_cache

# Define the SQLAlchemy Base
Base = declarative_base()
company_list = []

JOB_COLUMNS = [
    "job_title",
    "company_name",
    "company_logo",
    "salary",
    "posted_date",
    "experience",
    "location",
    "apply_link",
    "description",
    "data_source"
]


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


# Database setup function
def setup_database():
    """Initialize the database connection and tables"""
    try:
        # Create database engine - adjust connection string as needed
        engine = create_engine('postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres', echo=False)
        
        # Create tables
        Base.metadata.create_all(engine)
        
        # Create session factory
        Session = sessionmaker(bind=engine)
        
        return engine, Session
    except Exception as e:
        error_message = f"Failed to set up database: {str(e)}"
        notify_failure(error_message, "setup_database")
        raise


def get_chat_id(TOKEN: str) -> str:
    '''Get the chatid for our telegram bot'''
    
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = requests.get(url)
        info = response.json()
        chat_id = info['result'][0]['message']['chat']['id']
        return chat_id
    except Exception as e:
        print(f"Failed to get chat ID: {e}")
        return None


def send_message(TOKEN: str, message: str, chat_id: str):
    '''Send notification to bot'''

    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)

        if response.status_code == 200:
            print("Sent message successfully")
        else:
            print(f"Message not sent. Status code: {response.status_code}")
    except Exception as e:
        print(f"Failed to send message: {e}")


TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None


def notify_failure(error_message, location="Unknown"):
    """Send failure notification via Telegram"""
    global chat_id, TOKEN
    
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
        
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)


def get_company_list():
    global company_list
    try:
        df = pd.read_csv(r"C:\Users\birap\Downloads\2025-04-04_-_Worker_and_Temporary_Worker.csv")
        company_list = list(df['Organisation Name'])
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise


async def get_total_jobs():
    """Get the total number of jobs available on the site using async Playwright"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            await page.goto("https://www.cv-library.co.uk/jobs-in-uk", wait_until="domcontentloaded")
            await page.wait_for_selector("div.search-nav-actions__left p", timeout=10000)
            content = await page.content()
            
            await context.close()
            await browser.close()
            
            soup = BeautifulSoup(content, 'html.parser')
            total_jobs_text = soup.select_one("div.search-nav-actions__left p").get_text()
            match = re.search(r"Search ([\d,]+) jobs", total_jobs_text)
            
            if match:
                return int(match.group(1).replace(",", ""))
            return 0
    except Exception as e:
        error_message = f"Failed to get total jobs: {str(e)}"
        notify_failure(error_message, "get_total_jobs")
        return 0


def parse_date(date_str):
    """Parse date string using multiple potential formats, handling empty strings"""
    # Handle empty or None date strings
    if not date_str or date_str.strip() == '':
        return datetime.now().date()
        
    formats = [
        "%d/%m/%Y",                 # 19/04/2025
        "%Y-%m-%dT%H:%M:%SZ",       # 2025-04-19T17:49:01Z
        "%Y-%m-%dT%H:%M:%S.%fZ",    # 2025-04-22T16:11:27.096Z (with milliseconds)
        "%Y-%m-%d %H:%M:%S",        # 2025-04-19 17:49:01
        "%Y-%m-%d"                  # 2025-04-19
    ]
    
    for date_format in formats:
        try:
            return datetime.strptime(date_str, date_format).date()
        except ValueError:
            continue
    
    # If all parsing attempts fail, log and return today's date
    print(f"Failed to parse date '{date_str}' with any known format")
    return datetime.now().date()

import json
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def extract_description_async(url):
    """Extract job description from JSON-LD using Playwright's Async API"""
    if not url:
        return None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            await page.goto(url, timeout=30000)

            content = await page.content()

            await context.close()
            await browser.close()

            soup = BeautifulSoup(content, 'html.parser')

            # Find all <script> tags with type="application/ld+json"
            scripts = soup.find_all('script', type='application/ld+json')

            for script in scripts:
                try:
                    data = json.loads(script.string)
                    
                    # Check if it's a JobPosting type
                    if data.get('@type') == 'JobPosting':
                        description = data.get('description', None)
                        if description:
                            return description
                except json.JSONDecodeError:
                    continue  # Skip if not a valid JSON

            return None

    except Exception as e:
        print(f"Failed to extract description from {url}: {e}")
        return None


async def scrape_page_async(page_num):
    """Scrape a single page of job listings using async Playwright"""
    jobs_on_page = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            url = f'https://www.cv-library.co.uk/permanent-jobs-in-uk?perpage=100&page={page_num}&distance=750&salary_annual=4&salary_annual=7&salary_annual=5&salary_annual=8&salary_annual=6&salary_annual=3&us=1'
            print(f"Scraping page {page_num}...")
            
            await page.goto(url, wait_until="domcontentloaded")
            content = await page.content()
            
            # Close browser after getting the page content
            await context.close()
            await browser.close()
            
            soup = BeautifulSoup(content, "html.parser")
            job_cards = soup.select("ol#searchResults li.results__item")
            
            for job in job_cards:
                try:
                    section_element = job.find("article")

                    if not section_element:
                        continue

                    title = section_element.get("data-job-title", "")
                    company = section_element.get("data-company-name", "")
                    location = section_element.get("data-job-location", "")
                    salary = section_element.get("data-job-salary", "")
                    job_type = section_element.get("data-job-type", "")
                    date_posted = section_element.get("data-job-posted", "")
                    job_id = section_element.get("data-job-id", "")
                    industry = section_element.get("data-job-industry", "")

                    # Ensure job_id is not empty
                    if not job_id:
                        # Generate a unique ID using combination of title, company, timestamp
                        job_id = f"{title}_{company}_{int(time.time())}"

                    job_url = f'https://www.cv-library.co.uk/job/{job_id}' if job_id else ""

                    company_logo_element = job.select_one("img.job__logo")
                    company_logo = company_logo_element.get("data-src", "") if company_logo_element else ""

                    apply_tag = job.select_one('a.cvl-btn[href*="/apply"]')

                    base_url = "https://www.cv-library.co.uk"

                    if apply_tag and 'href' in apply_tag.attrs:
                        apply_link = apply_tag['href']
                        
                        if apply_link.startswith('/'):
                            apply_link = base_url + apply_link

                        parsed_url = urlparse(apply_link)
                        path_parts = parsed_url.path.split('/')

                        if 'apply' in path_parts:
                            apply_index = path_parts.index('apply')
                            new_path = '/'.join(path_parts[:apply_index]) + '/'
                        else:
                            new_path = parsed_url.path

                        apply_link = f"{parsed_url.scheme}://{parsed_url.netloc}{new_path}"
                    else:
                        apply_link = job_url

                    ingestion_time = datetime.utcnow().isoformat()
                    description = None
                    if company in company_list:
                        description = await extract_description_async(apply_link)

                    # Parse the date posted using our flexible parser
                    posted_date_obj = parse_date(date_posted)

                    # Mandatory fields validation
                    if not title:
                        title = "Untitled Position"

                    if not company:
                        company = "Unknown Company"

                    if '/hour' in salary.lower() or 'day' in salary.lower() or '/hourly' in salary.lower():
                        continue

                    if company in company_list:
                        jobs_on_page.append({
                            "job_title": title,
                            "experience": "",
                            "salary": salary or "Not specified",
                            "location": location or "Not specified",
                            "job_type": job_type or "Not specified",
                            "url": job_url,
                            "company_name": company,
                            "description": description or "No description available",
                            "posted_date": posted_date_obj,
                            "company_logo": company_logo,
                            "apply_link": apply_link,
                            "country": "UK",
                            "data_source": "cv_library",
                            "ingestion_timestamp": ingestion_time,
                            "external_job_id": job_id
                        })

                except Exception as e:
                    print(f"Error parsing job on page {page_num}: {e}")
    
    except Exception as e:
        error_message = f"Error scraping page {page_num}: {str(e)}"
        print(error_message)
        notify_failure(error_message, f"scrape_page_async({page_num})")
    
    return jobs_on_page


def batch_upsert_jobs(job_list, session, data_source='cv_library'):
    """Batch insert job listings into the database after deleting existing records with the same source"""
    try:
        if not job_list:
            print("No jobs to insert")
            return 0, 0

        # Delete existing records from same data source
        delete_count = session.query(Job).filter(Job.data_source == data_source).delete()
        print(f"Deleted {delete_count} existing records from source: {data_source}")

        job_records = []
        for raw_job in job_list:
            job_data = {}

            for col in JOB_COLUMNS:
                if col in raw_job:
                    job_data[col] = raw_job[col]
                elif col == "posted_date":
                    job_data[col] = datetime.now().date()
                elif col in ["job_title", "company_name", "apply_link", "data_source", "description"]:
                    job_data[col] = f"Default {col}"
                else:
                    job_data[col] = None

            job_records.append(job_data)

        if job_records:
            session.bulk_insert_mappings(Job, job_records)
            session.commit()

        return len(job_records), delete_count

    except Exception as e:
        session.rollback()
        error_message = f"Database batch insert failed: {str(e)}\n{traceback.format_exc()}"
        notify_failure(error_message, "batch_upsert_jobs")
        
        # Try to save to CSV as a fallback
        try:
            df = pd.DataFrame(job_list)
            os.makedirs('data/backup', exist_ok=True)
            backup_file = f'data/backup/failed_db_insert_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(backup_file, index=False)
            print(f"Failed data saved to backup CSV: {backup_file}")
        except Exception as csv_err:
            print(f"Failed to create backup CSV: {csv_err}")
            
        raise


async def get_job_listings_async(max_workers=15, max_pages=None):
    """Get job listings using async concurrency"""
    job_list = []
    start_time = time.time()

    try:
        total_jobs = await get_total_jobs()
        print(f"Total jobs found: {total_jobs}")
        
        if total_jobs == 0:
            notify_failure("No jobs found or failed to retrieve total job count", "get_job_listings_async")
            return []
            
        total_pages = (total_jobs // 100) + (1 if total_jobs % 100 != 0 else 0)
        
        if max_pages and max_pages < total_pages:
            total_pages = max_pages
            print(f"Limiting to {max_pages} pages")
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_workers)
        
        async def limited_scrape(page_num):
            async with semaphore:
                return await scrape_page_async(page_num)
        
        # Create tasks for all pages
        tasks = [limited_scrape(page_num) for page_num in range(1, total_pages + 1)]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Flatten results
        for page_results in results:
            if page_results:
                job_list.extend(page_results)
                print(f"Added {len(page_results)} jobs from a page")
        
        elapsed_time = time.time() - start_time
        print(f"Scraping completed in {elapsed_time:.2f} seconds")
        print(f"Total jobs scraped: {len(job_list)}")
        
        # Notify if we got very few jobs (potential failure)
        if len(job_list) < 10 and total_jobs > 10:
            notify_failure(f"Only {len(job_list)} jobs scraped out of {total_jobs} total jobs", "get_job_listings_async")
            
        return job_list
        
    except Exception as e:
        error_message = f"Failed in get_job_listings_async: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        notify_failure(error_message, "get_job_listings_async")
        return []


async def main_async():
    try:
        # Initialize chat_id at startup
        global chat_id
        chat_id = get_chat_id(TOKEN)
        
        # Send startup notification
        if chat_id:
            send_message(TOKEN, f"🚀 CV Library scraper started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", chat_id)
        
        # Set up database
        engine, Session = setup_database()
        session = Session()
        
        # Get company list and scrape jobs
        get_company_list()
        job_listings = await get_job_listings_async(max_workers=15, max_pages=None)
        
        if not job_listings:
            notify_failure("No job listings returned from scraper", "main_async")
            return
        
        # Use batch insert instead of upsert
        inserted, deleted = batch_upsert_jobs(job_listings, session)
        
        # Also save to CSV for backup
        df = pd.DataFrame(job_listings)
        
        # Save data
        try:
            os.makedirs('data', exist_ok=True)
            df.to_csv('data/sample_data.csv', index=False)
            
            # Send success notification
            if chat_id:
                success_message = (
                    f"✅ CV Library scraper completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"Scraped {len(job_listings)} jobs\n"
                    f"Database: {deleted} old jobs deleted, {inserted} new jobs inserted"
                )
                send_message(TOKEN, success_message, chat_id)
                
        except Exception as e:
            error_message = f"Failed to save data: {str(e)}"
            notify_failure(error_message, "data_saving")
            
    except Exception as e:
        error_message = f"Critical failure in main_async: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        notify_failure(error_message, "main_async")
    finally:
        # Close database session
        if 'session' in locals():
            session.close()


if __name__ == "__main__":
    # Run the async main function using asyncio.run()
    asyncio.run(main_async())