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
from rnet import Client, Impersonate
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import create_engine, Column, Integer, String, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []
company_name_map = {}

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
        df['Clean Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Clean Name'])
        
        # Create a mapping from raw names to cleaned names for reference
        for idx, row in df.iterrows():
            original = row['Organisation Name']
            cleaned = row['Clean Name']
            company_name_map[cleaned] = original
            
        print(f"Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise

def is_company_in_list(company_name):
    """Checks if a company name is in the target list using improved matching."""
    try:
        if not company_name:
            return False
        
        clean_company = clean_name(company_name)
        
        # Exact match
        if clean_company in company_list:
            return True
            
        # Try partial matching for companies with longer names
        for target_company in company_list:
            # If either name contains the other completely
            if clean_company in target_company or target_company in clean_company:
                # Only match if the contained part is substantial (at least 5 chars)
                if len(clean_company) >= 5 and len(target_company) >= 5:
                    print(f"Fuzzy match found: '{company_name}' matches '{company_name_map.get(target_company, target_company)}'")
                    return True
        
        return False
    except Exception as e:
        error_message = f"Failed to check company in list: {str(e)}"
        notify_failure(error_message, "is_company_in_list")
        return False


def remove_duplicates(jobs_list):
    """Removes duplicate job listings based on job URL."""
    try:
        unique_jobs = []
        seen_urls = set()
        
        for job in jobs_list:
            # Use job URL as unique identifier
            job_url = job.get('job_url')
            
            if not job_url:
                # If no URL (shouldn't happen), use combo of title and company
                job_identifier = f"{job.get('title', '')}_{job.get('company', '')}_{job.get('location', '')}"
            else:
                job_identifier = job_url
                
            if job_identifier not in seen_urls:
                seen_urls.add(job_identifier)
                unique_jobs.append(job)
        
        print(f"Removed {len(jobs_list) - len(unique_jobs)} duplicate jobs")
        return unique_jobs
    except Exception as e:
        error_message = f"Failed to remove duplicates: {str(e)}"
        notify_failure(error_message, "remove_duplicates")
        return jobs_list  # Return original list if deduplication fails


# Helper function for human-like waiting
async def human_delay(min_seconds=1, max_seconds=3):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def extract_job_description(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    script_tag = soup.find("script", type="application/ld+json")
    
    if not script_tag:
        return None

    try:
        job_json = json.loads(script_tag.string)
        raw_description = job_json.get("description", "")
        decoded_description = html.unescape(raw_description)
        return decoded_description
    except json.JSONDecodeError:
        return None


async def parse_url(url):
    client = Client(impersonate=Impersonate.Firefox136)
    resp = await client.get(url)
    print("Status Code: ", resp.status_code)
    
    content = await resp.text()

    soup = BeautifulSoup(content, "html.parser")
    description = extract_job_description(content)

    try:
        external_url = soup.find('code', id='applyUrl')
        if external_url:
            external_url = external_url.string
        else:
            external_url = url
    except:
        external_url = url

    return external_url, description


async def get_external_url(url):
    try:
        time.sleep(random.randint(1, 5))
        return await parse_url(url)
    except Exception as e:
        print(f"Error getting external URL: {e}")
        return url, None


def extract_job_details(html):
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
            
            # Print for debugging
            clean_company_name = clean_name(company_name)
            company_match = is_company_in_list(company_name)
            
            # Skip jobs from companies not in our target list
            if not company_match:
                continue
                
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

            job_data['job_url'] = url
            job_data['description'] = None
            job_listings.append(job_data)

        print(f"📊 Extraction stats: {matching_jobs}/{total_jobs} jobs matched target companies")
        return job_listings
    except Exception as e:
        error_message = f"Failed to extract job details: {str(e)}"
        notify_failure(error_message, "extract_job_details")
        return []


async def process_job_urls(job_listings):
    """Process all job URLs to get external URLs and descriptions asynchronously"""
    tasks = []
    for job in job_listings:
        if job.get('job_url'):
            tasks.append(get_external_url(job['job_url']))
    
    # Process all URLs concurrently for better performance
    if tasks:
        results = await asyncio.gather(*tasks)
        
        # Update job listings with results
        for i, (external_url, description) in enumerate(results):
            if i < len(job_listings):
                job_listings[i]['job_url'] = external_url.replace('"', '')
                job_listings[i]['description'] = description
    
    return job_listings


def insert_jobs_to_db(job_listings):
    """Delete all jobs with data_source='linkedin' and insert new jobs"""
    if not job_listings:
        logger.info("No jobs to insert into database")
        return 0
    
    inserted_count = 0
    
    try:
        session = Session()
        try:
            # Delete all existing records with data_source='linkedin'
            logger.info("Deleting all existing LinkedIn jobs from database...")
            deleted_count = session.query(Job).filter(Job.data_source == 'linkedin').delete()
            logger.info(f"Deleted {deleted_count} existing LinkedIn jobs")
            
            # Insert new records
            logger.info(f"Inserting {len(job_listings)} new jobs...")
            for job in job_listings:
                try:
                    # Create new job object
                    new_job = Job(
                        job_title=job.get('title', ''),
                        company_name=job.get('company', ''),
                        company_logo=job.get('logo_url', ''),
                        salary=job.get('salary', '') if job.get('salary') else None,
                        posted_date=job.get('posted_time', ''),
                        experience='Full-time',  # Default or extract from job data if available
                        location=job.get('location', '') if job.get('location') else None,
                        apply_link=job.get('job_url', ''),
                        description=job.get('description', ''),
                        data_source='linkedin'
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
        notify_failure(error_message, "close_popups")


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


async def load_all_jobs():
    global chat_id  # Declare chat_id as global within this function
    
    async with async_playwright() as p:
        # Launch browser with stealth mode
        browser = await p.chromium.launch(
            headless=True,  # Set to True for production
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
            // If needed, override more properties here
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
            
            # LinkedIn jobs search URL
            URL = "https://www.linkedin.com/jobs/search/?currentJobId=4215342655&f_E=4&f_JT=F&f_SB2=42&f_TPR=r604800&f_WT=1%2C3&geoId=101165590&keywords=&location=United%20Kingdom&origin=JOB_SEARCH_PAGE_JOB_FILTER"
            
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
            
            while True:
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
                jobs = extract_job_details(page_content)
                
                # Process the job URLs to get external URLs and descriptions
                jobs = await process_job_urls(jobs)
                
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


# Optional - Function to deduplicate an existing JSON file
def deduplicate_existing_json(filepath="linkedin_filtered_jobs.json"):
    """Removes duplicates from an existing JSON file."""
    global chat_id  # Declare chat_id as global within this function
    
    try:
        print(f"Deduplicating existing file: {filepath}")
        
        # Load existing data
        with open(filepath, 'r', encoding='utf-8') as file:
            existing_jobs = json.load(file)
            
        original_count = len(existing_jobs)
        print(f"Original job count: {original_count}")
        
        # Remove duplicates
        unique_jobs = remove_duplicates(existing_jobs)
        
        # Save deduped data
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(unique_jobs, file, indent=2, ensure_ascii=False)
            
        print(f"Deduplication complete: {original_count} → {len(unique_jobs)} jobs")
        return len(unique_jobs)
    except Exception as e:
        error_message = f"Error in deduplicate_existing_json: {str(e)}"
        print(error_message)
        notify_failure(error_message, "deduplicate_existing_json")
        return 0


if __name__ == "__main__":
    asyncio.run(main())