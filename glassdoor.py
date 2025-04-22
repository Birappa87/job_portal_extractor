from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import random
import json
import pandas as pd
import re
import os
import traceback
import requests
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dateutil import parser

company_list = []

# SQLAlchemy setup
Base = declarative_base()
engine = None
Session = None

# Define the database model - same schema as CV Library scraper
class Job(Base):
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    job_title = Column(String(255), nullable=False)
    company_name = Column(String(255), nullable=False)
    company_logo = Column(String, nullable=True)
    salary = Column(String(100), nullable=True)
    posted_date = Column(Date, nullable=False)
    experience = Column(String(100), nullable=True)
    location = Column(String(255), nullable=True)
    apply_link = Column(String, nullable=False)
    data_source = Column(String(180), nullable=False)

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
        else:
            print("Jobs table already exists")
            
        # Test connection
        with engine.connect() as conn:
            print("Database connection test successful")
            
    except Exception as e:
        error_message = f"Failed to initialize database: {str(e)}"
        print(error_message)
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

TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None

def notify_failure(error_message, location="Unknown"):
    """Sends a failure notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ GLASSDOOR SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
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
        df = pd.read_csv(r"C:\\Users\\birap\\Downloads\\2025-04-04_-_Worker_and_Temporary_Worker.csv")
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Organisation Name'])
        print(f"Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise

def parse_date(date_str):
    """Parse date string in various formats to a datetime.date object"""
    try:
        if not date_str or date_str == "N/A":
            return datetime.now().date()
        
        # Handle "X days ago" format
        days_ago_match = re.search(r'(\d+)d ago', date_str)
        if days_ago_match:
            days = int(days_ago_match.group(1))
            return (datetime.now() - pd.Timedelta(days=days)).date()
            
        # Handle "Today" and "Just posted"
        if date_str.lower() in ["today", "just posted"]:
            return datetime.now().date()
            
        # Try parsing the date
        return parser.parse(date_str).date()
    except Exception:
        # If parsing fails, return current date
        return datetime.now().date()

def insert_jobs_to_db(job_listings):
    """Insert or update job listings in PostgreSQL database"""
    if not job_listings:
        print("No jobs to insert into database")
        return 0
    
    total_count = 0
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    try:
        print(f"Starting upsert operation for {len(job_listings)} jobs...")
        
        # First, check if the table is empty to optimize the process
        session = Session()
        try:
            table_empty = session.query(Job).first() is None
            print(f"Table status: {'Empty' if table_empty else 'Contains data'}")
        except Exception as e:
            print(f"Error checking table status: {str(e)}")
            table_empty = False  # Assume table has data if we can't check
        finally:
            session.close()
        
        # Process jobs in smaller batches
        batch_size = 50
        for i in range(0, len(job_listings), batch_size):
            batch = job_listings[i:i+batch_size]
            session = Session()  # Create a new session for each batch
            batch_inserted = 0
            batch_updated = 0
            
            try:
                # If table is empty, we can skip existence checks and just insert everything
                if table_empty and i == 0:  # Only for the first batch
                    print("Fast path: Inserting all records into empty table")
                    for job in batch:
                        try:
                            posted_date = parse_date(job.get('posted_date', ''))
                            
                            new_job = Job(
                                job_title=job.get('title', ''),
                                company_name=job.get('company', ''),
                                company_logo=job.get('company_logo', ''),
                                salary=job.get('salary', ''),
                                posted_date=posted_date,
                                experience=job.get('experience', ''),
                                location=job.get('location', ''),
                                apply_link=job.get('apply_link', ''),
                                data_source=job.get('source', 'glassdoor')
                            )
                            session.add(new_job)
                            batch_inserted += 1
                        except Exception as e:
                            error_count += 1
                            print(f"Error processing job object: {str(e)}")
                    
                    # After first batch, we'll use the standard approach
                    table_empty = False
                else:
                    # Standard approach - check existence for each record
                    for job in batch:
                        try:
                            # Parse the posted date
                            posted_date = parse_date(job.get('posted_date', ''))
                            
                            # Use more explicit query to check if record exists
                            job_title = job.get('title', '')
                            company_name = job.get('company', '')
                            data_source = job.get('source', 'glassdoor')
                            
                            # Debug output to see what we're checking for
                            if i == 0 and batch_inserted + batch_updated < 3:
                                print(f"Checking for existence: '{job_title}' at '{company_name}' from '{data_source}'")
                            
                            # Explicit query with output count for troubleshooting
                            query = session.query(Job).filter(
                                Job.job_title == job_title,
                                Job.company_name == company_name,
                                Job.data_source == data_source
                            )
                            
                            # Debug the first few queries if needed
                            if i == 0 and batch_inserted + batch_updated < 3:
                                count = query.count()
                                print(f"Found {count} matching records")
                            
                            existing_job = query.first()
                            
                            if existing_job:
                                # Update existing record
                                existing_job.company_logo = job.get('company_logo', '')
                                existing_job.salary = job.get('salary', '')
                                existing_job.posted_date = posted_date
                                existing_job.experience = job.get('experience', '')
                                existing_job.location = job.get('location', '')
                                existing_job.apply_link = job.get('apply_link', '')
                                batch_updated += 1
                            else:
                                # Create new Job object
                                new_job = Job(
                                    job_title=job_title,
                                    company_name=company_name,
                                    company_logo=job.get('company_logo', ''),
                                    salary=job.get('salary', ''),
                                    posted_date=posted_date,
                                    experience=job.get('experience', ''),
                                    location=job.get('location', ''),
                                    apply_link=job.get('apply_link', ''),
                                    data_source=data_source
                                )
                                session.add(new_job)
                                batch_inserted += 1
                            
                        except Exception as e:
                            error_count += 1
                            print(f"Error processing job object: {str(e)}")
                            # Continue processing other jobs in the batch
                
                # Commit the batch
                session.commit()
                inserted_count += batch_inserted
                updated_count += batch_updated
                total_count += batch_inserted + batch_updated
                print(f"Batch completed: {batch_inserted} inserted, {batch_updated} updated")
                
            except Exception as e:
                error_message = f"Database batch upsert error: {str(e)}"
                print(error_message)
                notify_failure(error_message, "insert_jobs_to_db_batch")
                session.rollback()  # Important: roll back the transaction
            finally:
                session.close()  # Always close the session
        
        print(f"Database operation completed: {inserted_count} new jobs inserted, {updated_count} jobs updated, {error_count} errors")
        
    except Exception as e:
        error_message = f"Database upsert error: {str(e)}"
        print(error_message)
        notify_failure(error_message, "insert_jobs_to_db")
    
    return total_count

def extract_data(content, region):
    """Extracts job data from Glassdoor page HTML."""
    try:
        soup = BeautifulSoup(content, 'html.parser')
        job_element = soup.find('ul', class_='JobsList_jobsList__lqjTr')
        if not job_element:
            print("No job list found.")
            notify_failure(f"No job list found in {region}", "extract_data")
            return []

        job_cards = job_element.find_all('li', class_='JobsList_jobListItem__wjTHv')
        print(f"Total Jobs Extracted: {len(job_cards)}\n")

        all_jobs = []
        
        for job in job_cards:
            try:
                job_card_wrapper = job.find('div', class_='JobCard_jobCardWrapper__vX29z')
                if not job_card_wrapper:
                    continue
                
                title_element = job_card_wrapper.find('a', class_='JobCard_jobTitle__GLyJ1')
                if not title_element:
                    continue
                
                title = title_element.get_text(strip=True)
                job_url = title_element.get('href', '')
                if not job_url.startswith("http"):
                    job_url = f"https://www.glassdoor.co.uk{job_url}"

                company_element = job_card_wrapper.find('span', class_='EmployerProfile_compactEmployerName__9MGcV')
                company = company_element.get_text(strip=True) if company_element else "N/A"

                location_element = job_card_wrapper.find('div', class_='JobCard_location__Ds1fM')
                location = location_element.get_text(strip=True) if location_element else "N/A"

                salary_element = job_card_wrapper.find('div', class_='JobCard_salaryEstimate__QpbTW')
                salary = salary_element.get_text(strip=True) if salary_element else "N/A"

                date_posted_element = job_card_wrapper.find('div', class_='JobCard_listingAge__jJsuc')
                date_posted = date_posted_element.get_text(strip=True) if date_posted_element else "N/A"

                job_type = "Easy Apply" if job_card_wrapper.find('div', class_='JobCard_easyApplyTag__5vlo5') else "Standard"

                logo_container = job_card_wrapper.find('div', class_='EmployerProfile_profileContainer__63w3R')
                company_logo = "N/A"
                if logo_container:
                    img_element = logo_container.find('img', class_='avatar-base_Image__2RcF9')
                    if img_element:
                        company_logo = img_element.get('src', 'N/A')

                description_element = job_card_wrapper.find('div', class_='JobCard_jobDescriptionSnippet__l1tnl')
                description = description_element.get_text(strip=True) if description_element else ""
                description = description.replace('&hellip;', '...')

                cleaned_company = clean_name(company)
                if cleaned_company in company_list:
                    ingestion_time = datetime.utcnow().isoformat()
                    
                    job_data = {
                        "title": title,
                        "experience": "",
                        "salary": salary,
                        "location": location,
                        "job_type": job_type,
                        "url": job_url,
                        "company": company,
                        "description": description,
                        "posted_date": date_posted,
                        "company_logo": company_logo,
                        "apply_link": job_url,
                        "country": "UK",
                        "source": "glassdoor",
                        "ingestion_timestamp": ingestion_time,
                        "region": region
                    }

                    all_jobs.append(job_data)
            except Exception as e:
                continue

        print(f"✅ Total jobs collected from {region}: {len(all_jobs)}\n")
        return all_jobs
    except Exception as e:
        error_message = f"Failed to extract data: {str(e)}"
        notify_failure(error_message, f"extract_data({region})")
        return []

def main():
    """Main function to coordinate the Glassdoor scraping process."""
    try:
        global chat_id, TOKEN
        chat_id = get_chat_id(TOKEN)
        if chat_id:
            send_message(TOKEN, f"🚀 Glassdoor scraper started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", chat_id)
        
        # Initialize database
        init_db()
        
        # Get company list
        get_company_list()

        urls = {
            "england": "https://www.glassdoor.co.uk/Job/england-uk-jobs-SRCH_IL.0,10_IS7287.htm?maxSalary=9000000&minSalary=250000",
            "scotland": "https://www.glassdoor.co.uk/Job/scotland-uk-jobs-SRCH_IL.0,11_IS7289.htm?maxSalary=9000000&minSalary=250000",
            "wales": "https://www.glassdoor.co.uk/Job/wales-uk-jobs-SRCH_IL.0,8_IS7290.htm?maxSalary=9000000&minSalary=250000",
            "northern_ireland": "https://www.glassdoor.co.uk/Job/northern-ireland-uk-jobs-SRCH_IL.0,19_IS7288.htm?maxSalary=9000000&minSalary=250000"
        }

        all_data = []

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    locale="en-GB,en;q=0.5",
                    geolocation={"latitude": 51.509865, "longitude": -0.118092},
                    permissions=["geolocation"],
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
                page = context.new_page()

                for region, url in urls.items():
                    try:
                        print(f"\n🚀 Scraping jobs for: {region.upper()}")
                        page.goto(url)
                        time.sleep(3)

                        prev_count = 0
                        stable_rounds = 0

                        while True:
                            try:
                                close_btn = page.query_selector("button.CloseButton")
                                if close_btn:
                                    close_btn.click()
                                    time.sleep(1)
                            except Exception as e:
                                pass

                            for _ in range(3):
                                page.mouse.wheel(0, 2000)
                                time.sleep(1)

                            content = page.content()
                            soup = BeautifulSoup(content, 'html.parser')
                            current_jobs = soup.select("ul.JobsList_jobsList__lqjTr li")
                            current_count = len(current_jobs)
                            print(f"Currently loaded: {current_count} jobs")

                            try:
                                button = page.query_selector('button[data-test="load-more"]')
                                if button:
                                    print("Clicking 'Show more jobs'...")
                                    button.click()
                                    time.sleep(random.uniform(1.5, 4))
                                else:
                                    print("No 'Show more jobs' button found.")
                            except Exception as e:
                                print(f"Error clicking 'Show more jobs': {e}")

                            if current_count == prev_count:
                                stable_rounds += 1
                                if stable_rounds >= 3:
                                    break
                            else:
                                stable_rounds = 0
                                prev_count = current_count

                        final_content = page.content()
                        region_jobs = extract_data(final_content, region)
                        all_data.extend(region_jobs)

                        if len(region_jobs) < 5 and current_count > 10:
                            notify_failure(f"Only {len(region_jobs)} jobs collected from {region} after filtering", f"region_scraping({region})")

                    except Exception as e:
                        error_message = f"Error scraping {region}: {str(e)}"
                        print(error_message)
                        notify_failure(error_message, f"region_scraping({region})")

                browser.close()

            except Exception as e:
                error_message = f"Browser error: {str(e)}"
                print(error_message)
                notify_failure(error_message, "browser_setup")

        try:
            if len(all_data) == 0:
                error_message = "No jobs collected from any region"
                print(error_message)
                notify_failure(error_message, "data_collection")
                return

            # Save to JSON file
            with open("glassdoor_all_uk.json", "w+", encoding="utf-8") as final_file:
                json.dump(all_data, final_file, indent=2)

            # Save to CSV file
            df = pd.DataFrame(all_data)
            columns = [
                "title", "experience", "salary", "location", "job_type",
                "url", "company", "description", "posted_date", "company_logo", 
                "apply_link", "country", "source", "ingestion_timestamp"
            ]
            existing_columns = [col for col in columns if col in df.columns]
            df = df[existing_columns]
            os.makedirs("data", exist_ok=True)
            df.to_csv('data/glassdoor_data.csv', index=False)
            
            # Insert jobs into database
            inserted_count = insert_jobs_to_db(all_data)

            if chat_id:
                success_message = (
                    f"✅ Glassdoor scraper completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"Scraped {len(all_data)} jobs across {len(urls)} UK regions\n"
                    f"Inserted {inserted_count} jobs into database"
                )
                send_message(TOKEN, success_message, chat_id)

        except Exception as e:
            error_message = f"Failed to save data: {str(e)}\n{traceback.format_exc()}"
            print(error_message)
            notify_failure(error_message, "data_saving")

    except Exception as e:
        error_message = f"Critical failure in main: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        notify_failure(error_message, "main")

if __name__ == "__main__":
    main()