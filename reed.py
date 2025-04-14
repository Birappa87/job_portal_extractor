from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import requests
import traceback
import json
import os
import time
import logging
import sys
from typing import List, Dict, Optional, Union, Any
from requests.exceptions import RequestException, Timeout, ConnectionError
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reed_scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("reed_scraper")

# Configuration
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
DEFAULT_CHAT_ID = None
OUTPUT_FILE = "reed_jobs.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
REQUEST_TIMEOUT = 30  # seconds
PAGE_LOAD_TIMEOUT = 45000  # milliseconds
DEFAULT_CSV_PATH = "/Users/joshuadsouza/Desktop/ALL_IN_ONE/FreeLance/freelance/job_portal_extractor/data/2025-04-04_-_Worker_and_Temporary_Worker.csv"
DEFAULT_BASE_URL = "https://www.reed.co.uk/jobs/full-time-jobs-in-united-kingdom?sortBy=displayDate&salaryFrom=28000&dateCreatedOffSet=today"

company_list = []

class ScrapeException(Exception):
    """Custom exception for scraping failures"""
    pass

def clean_name(name: str) -> str:
    """Clean a company name string for comparison."""
    if not isinstance(name, str):
        return ""
    return re.sub(r'[^\w\s]', '', name).lower().strip()

def get_chat_id(token: str, max_retries: int = 3) -> Optional[str]:
    """Get Telegram chat ID with retry mechanism."""
    for attempt in range(max_retries):
        try:
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            if not response.ok:
                logger.error(f"Failed to get chat ID (HTTP {response.status_code}): {response.text}")
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return None
                
            info = response.json()
            if not info.get('ok', False):
                logger.error(f"Telegram API error: {info.get('description', 'Unknown error')}")
                return None
                
            if not info.get('result') or len(info['result']) == 0:
                logger.warning("No updates found in Telegram bot. Send a message to the bot first.")
                return None
                
            return info['result'][0]['message']['chat']['id']
        except (RequestException, KeyError, IndexError, ValueError) as e:
            logger.error(f"Failed to get chat ID (attempt {attempt+1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None

def send_message(token: str, message: str, chat_id: str, max_retries: int = 3) -> bool:
    """Send a Telegram message with retry mechanism."""
    if not chat_id:
        logger.warning("No chat ID available to send message")
        return False
        
    for attempt in range(max_retries):
        try:
            # URL encode the message to handle special characters
            encoded_message = requests.utils.quote(message)
            url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={encoded_message}"
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            
            if response.ok:
                logger.info("✅ Telegram message sent successfully")
                return True
            else:
                logger.error(f"⚠️ Telegram error (HTTP {response.status_code}): {response.text}")
                # Only retry if it's likely to succeed on retry (not a permanent error)
                if response.status_code >= 500 and attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return False
                
        except (ConnectionError, Timeout) as e:
            logger.error(f"Network error sending telegram message (attempt {attempt+1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                return False
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            return False
    
    return False

def notify_failure(error_message: str, location: str = "Unknown", chat_id: Optional[str] = None) -> None:
    """Send notification about scraper failure."""
    global DEFAULT_CHAT_ID
    
    if chat_id is None:
        chat_id = DEFAULT_CHAT_ID
        
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
        DEFAULT_CHAT_ID = chat_id
        
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ REED SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)
    else:
        logger.critical(f"Could not notify about failure: {error_message} at {location}")

def get_company_list(path: str) -> List[str]:
    """Load and clean company list from CSV file."""
    global company_list
    
    if not os.path.exists(path):
        error_msg = f"CSV file not found: {path}"
        logger.error(error_msg)
        notify_failure(error_msg, "get_company_list")
        return []
        
    try:
        df = pd.read_csv(path)
        
        if 'Organisation Name' not in df.columns:
            error_msg = "Required column 'Organisation Name' not found in CSV"
            logger.error(error_msg)
            notify_failure(error_msg, "get_company_list")
            return []
            
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = [name for name in list(df['Organisation Name']) if name]  # Filter out empty names
        
        logger.info(f"✅ Loaded {len(company_list)} companies from CSV")
        return company_list
        
    except pd.errors.EmptyDataError:
        error_msg = f"CSV file is empty: {path}"
        logger.error(error_msg)
        notify_failure(error_msg, "get_company_list")
        return []
        
    except Exception as e:
        error_msg = f"CSV Load Error: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        notify_failure(error_msg, "get_company_list")
        return []

def get_total_pages(html: str) -> int:
    """Extract the total number of pages from search results."""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.select(".pagination .page-item .page-link")
        
        if not pagination:
            logger.warning("No pagination found, defaulting to 1 page")
            return 1
            
        max_page = 1
        
        # Find the highest page number
        for page_link in pagination:
            # Skip links with no text content (like Next/Previous buttons)
            if page_link.get_text(strip=True).isdigit():
                page_num = int(page_link.get_text(strip=True))
                max_page = max(max_page, page_num)
        
        logger.info(f"Found {max_page} total pages")
        return max_page
        
    except Exception as e:
        logger.error(f"Error parsing pagination: {str(e)}")
        return 1  # Default to 1 page on error

def fetch_reed_jobs_page(url: str, max_retries: int = MAX_RETRIES) -> str:
    """Fetch page content with Playwright with retry mechanism."""
    for attempt in range(max_retries):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                
                # Add some randomization to appear more human-like
                page.set_default_timeout(PAGE_LOAD_TIMEOUT)
                
                # Handle potential navigation errors
                page.goto(url, wait_until="domcontentloaded")
                
                # Wait for job cards to appear
                try:
                    page.wait_for_selector("article[data-qa='job-card']", timeout=PAGE_LOAD_TIMEOUT)
                except PlaywrightTimeoutError:
                    logger.warning(f"Timeout waiting for job cards on {url}")
                    # Check if page contains any error messages or captcha
                    page_content = page.content()
                    if "captcha" in page_content.lower() or "blocked" in page_content.lower():
                        logger.error("Detected captcha or IP blocking")
                        raise ScrapeException("Detected captcha or IP blocking")
                    # Continue anyway, we'll validate content later
                
                html = page.content()
                browser.close()
                
                # Validate that we got some useful content
                if "job-card" not in html:
                    logger.warning(f"No job cards found in page content on attempt {attempt+1}")
                    if attempt < max_retries - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))  # Incremental backoff
                        continue
                    raise ScrapeException("Failed to fetch job listings")
                    
                return html
                
        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout error on attempt {attempt+1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise ScrapeException(f"Timeout error after {max_retries} attempts: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error fetching page on attempt {attempt+1}: {str(e)}")
            logger.error(traceback.format_exc())
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise ScrapeException(f"Failed to fetch page after {max_retries} attempts: {str(e)}")

def parse_job_date(date_text: str) -> str:
    """Parse and normalize job posting date."""
    if not date_text:
        return None
        
    try:
        # Remove "Posted " prefix if present and any "by Company" suffix
        clean_text = date_text.replace("Posted ", "").split("by")[0].strip()
        
        today = datetime.now().date()
        
        if "today" in clean_text.lower():
            return today.isoformat()
            
        if "yesterday" in clean_text.lower():
            return (today - timedelta(days=1)).isoformat()
            
        # Handle "X days ago" format
        if "days ago" in clean_text.lower():
            days = int(re.search(r'(\d+)', clean_text).group(1))
            return (today - timedelta(days=days)).isoformat()
            
        # Handle other formats as needed
        return clean_text
        
    except Exception as e:
        logger.warning(f"Could not parse date '{date_text}': {str(e)}")
        return date_text  # Return original if parsing fails

def extract_salary_range(salary_text: str) -> Dict[str, Union[float, None]]:
    """Extract structured salary information from text."""
    if not salary_text:
        return {"min": None, "max": None, "period": None, "currency": None, "original": None}
        
    result = {
        "min": None,
        "max": None,
        "period": None,
        "currency": "GBP",
        "original": salary_text
    }
    
    try:
        # Extract currency if present
        if "€" in salary_text:
            result["currency"] = "EUR"
        elif "$" in salary_text:
            result["currency"] = "USD"
            
        # Extract period
        if "per annum" in salary_text.lower() or "pa" in salary_text.lower():
            result["period"] = "annual"
        elif "per month" in salary_text.lower() or "pm" in salary_text.lower():
            result["period"] = "monthly"
        elif "per day" in salary_text.lower() or "pd" in salary_text.lower():
            result["period"] = "daily"
        elif "per hour" in salary_text.lower() or "ph" in salary_text.lower() or "per hr" in salary_text.lower():
            result["period"] = "hourly"
            
        # Extract amounts
        # Look for patterns like £30,000 - £40,000 or £30k - £40k
        amounts = re.findall(r'[\£\$\€]\s*(\d+[,\d]*\.?\d*|\d*\.?\d+)(?:k)?', salary_text)
        
        if len(amounts) >= 2:
            # Convert first amount to float
            min_amount = amounts[0].replace(',', '')
            if 'k' in salary_text.lower() and min_amount.isdigit():
                min_amount = float(min_amount) * 1000
            else:
                min_amount = float(min_amount)
                
            # Convert second amount to float
            max_amount = amounts[1].replace(',', '')
            if 'k' in salary_text.lower() and max_amount.isdigit():
                max_amount = float(max_amount) * 1000
            else:
                max_amount = float(max_amount)
                
            result["min"] = min_amount
            result["max"] = max_amount
        elif len(amounts) == 1:
            # Single amount mentioned
            amount = amounts[0].replace(',', '')
            if 'k' in salary_text.lower() and amount.isdigit():
                amount = float(amount) * 1000
            else:
                amount = float(amount)
                
            result["min"] = amount
            result["max"] = amount
    
    except Exception as e:
        logger.warning(f"Error parsing salary '{salary_text}': {str(e)}")
        
    return result

def parse_job_type(type_text: str) -> Dict[str, bool]:
    """Parse job type information into structured format."""
    result = {
        "full_time": False,
        "part_time": False,
        "contract": False,
        "permanent": False,
        "temporary": False,
        "original": type_text
    }
    
    if not type_text:
        return result
        
    type_lower = type_text.lower()
    
    if "full-time" in type_lower or "full time" in type_lower:
        result["full_time"] = True
        
    if "part-time" in type_lower or "part time" in type_lower:
        result["part_time"] = True
        
    if "contract" in type_lower:
        result["contract"] = True
        
    if "permanent" in type_lower:
        result["permanent"] = True
        
    if "temporary" in type_lower or "temp" in type_lower:
        result["temporary"] = True
        
    return result

def scrape_reed_jobs(base_url: str, pages: int = 5, region: str = "UK") -> List[Dict[str, Any]]:
    """Scrape Reed job listings across multiple pages."""
    all_jobs = []
    successful_pages = 0
    failed_pages = 0
    
    for page_no in range(1, pages + 1):
        logger.info(f"Scraping page {page_no} of {pages}")
        url = f"{base_url}&pageno={page_no}"
        
        try:
            html = fetch_reed_jobs_page(url)
            soup = BeautifulSoup(html, 'html.parser')
            job_cards = soup.find_all("article", class_="job-card_jobCard__MkcJD")
            
            if not job_cards:
                logger.warning(f"No job cards found on page {page_no}")
                failed_pages += 1
                if failed_pages >= 3:  # Stop if 3 consecutive pages fail
                    logger.error("Too many failed pages, stopping scrape")
                    break
                continue
                
            logger.info(f"Found {len(job_cards)} jobs on page {page_no}")
            failed_pages = 0  # Reset failed counter on success
            successful_pages += 1
            
            for card in job_cards:
                try:
                    title_el = card.select_one('[data-qa="job-card-title"]')
                    company_el = card.select_one('.job-card_jobResultHeading__postedBy__sK_25 a')
                    logo_el = card.select_one('img[data-qa="company-logo-image"]')
                    logo_url = logo_el['src'] if logo_el and logo_el.has_attr('src') else None
                    
                    # Extract job details
                    salary_text = location_text = type_text = None
                    for li in card.select('ul[data-qa="job-card-options"] > li'):
                        text = li.get_text(strip=True)
                        if '£' in text or '$' in text or '€' in text:
                            salary_text = text
                        elif any(keyword in text.lower() for keyword in ['full-time', 'part-time', 'contract', 'permanent', 'temporary']):
                            type_text = text
                        else:
                            location_text = text
                            
                    description_el = card.select_one('[data-qa="jobDescriptionDetails"]')
                    posted_el = card.select_one('.job-card_jobResultHeading__postedBy__sK_25')
                    posted_text = posted_el.get_text(strip=True).split('by')[0].strip() if posted_el else None
                    
                    company = company_el.text.strip() if company_el else None
                    title = title_el.text.strip() if title_el else None
                    
                    # Skip if no company or title
                    if not company or not title:
                        logger.warning(f"Skipping job with missing company or title: {company} - {title}")
                        continue
                        
                    cleaned_company = clean_name(company)
                    
                    # Check if company is in our target list
                    if not company_list or cleaned_company in company_list:
                        # salary_info = extract_salary_range(salary_text)
                        # job_type_info = parse_job_type(type_text)
                        # parsed_date = parse_job_date(posted_text)
                        
                        job_data = {
                            "title": title,
                            "experience": "",
                            "salary": salary_text,
                            # "salary_structured": salary_info,
                            "location": location_text,
                            "job_type": type_text,
                            # "job_type_structured": job_type_info,
                            "company": company,
                            "company_cleaned": cleaned_company,
                            "description": description_el.get_text(strip=True) if description_el else None,
                            "posted_date": posted_text,
                            # "posted_date_normalized": parsed_date,
                            "company_logo": logo_url,
                            "apply_link": "https://www.reed.co.uk" + title_el['href'] if title_el and title_el.has_attr('href') else None,
                            "country": "United Kingdom",
                            "source": "reed",
                            "ingestion_timestamp": datetime.utcnow().isoformat()
                        }
                        all_jobs.append(job_data)
                        
                except Exception as e:
                    logger.error(f"Error processing job card: {str(e)}")
                    continue
                    
        except ScrapeException as e:
            error_msg = f"Failed to scrape page {page_no}: {str(e)}"
            logger.error(error_msg)
            notify_failure(error_msg, f"scrape_reed_jobs - page {page_no}")
            failed_pages += 1
            if failed_pages >= 3:  # Stop if 3 consecutive pages fail
                logger.error("Too many failed pages, stopping scrape")
                break
                
        except Exception as e:
            error_msg = f"Unexpected error on page {page_no}: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            notify_failure(error_msg, f"scrape_reed_jobs - page {page_no}")
            failed_pages += 1
            if failed_pages >= 3:  # Stop if 3 consecutive pages fail
                logger.error("Too many failed pages, stopping scrape")
                break
                
        # Add a delay between pages to avoid rate limiting
        if page_no < pages:
            delay = RETRY_DELAY + (page_no % 3)  # Varying delay
            logger.info(f"Waiting {delay} seconds before next page...")
            time.sleep(delay)
            
    logger.info(f"Completed scraping {successful_pages} pages successfully, {failed_pages} pages failed")
    return all_jobs

def save_jobs_to_file(jobs: List[Dict[str, Any]], filename: str = OUTPUT_FILE) -> bool:
    """Save scraped jobs to JSON file with error handling."""
    try:
        # Create directory if it doesn't exist
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
            
        logger.info(f"💾 Saved {len(jobs)} jobs to {filename}")
        return True
        
    except PermissionError:
        error_msg = f"Permission denied when saving to {filename}"
        logger.error(error_msg)
        notify_failure(error_msg, "save_jobs_to_file")
        
        # Try with a fallback filename
        try:
            fallback_filename = f"reed_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(fallback_filename, "w", encoding="utf-8") as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved to fallback file: {fallback_filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save to fallback file: {str(e)}")
            return False
            
    except Exception as e:
        error_msg = f"Failed to save jobs to file: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        notify_failure(error_msg, "save_jobs_to_file")
        return False

def main(
    base_url: str = DEFAULT_BASE_URL,
    csv_path: str = DEFAULT_CSV_PATH,
    output_file: str = OUTPUT_FILE
) -> None:
    """Main function to run the Reed job scraper."""
    global DEFAULT_CHAT_ID
    
    start_time = datetime.now()
    logger.info(f"🚀 Reed scraper started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Get chat ID for notifications
        DEFAULT_CHAT_ID = get_chat_id(TOKEN)
        if DEFAULT_CHAT_ID:
            send_message(TOKEN, f"🚀 Reed scraper started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}", DEFAULT_CHAT_ID)
        else:
            logger.warning("Could not get Telegram chat ID, notifications will be disabled")
            
        # Load company list
        get_company_list(csv_path)
        
        # Check if company list is empty
        if not company_list:
            logger.warning("Company list is empty, will scrape all companies")
            
        # Get total pages
        try:
            logger.info(f"Fetching first page to determine total pages: {base_url}")
            first_page_html = fetch_reed_jobs_page(base_url)
            total_pages = get_total_pages(first_page_html)
        except Exception as e:
            logger.error(f"Failed to determine total pages: {str(e)}")
            total_pages = 5  # Default to 5 pages
            notify_failure(f"Failed to determine total pages: {str(e)}", "main")
            
        # Scrape jobs
        logger.info(f"Starting scrape of {total_pages} pages")
        jobs = scrape_reed_jobs(base_url, total_pages)
        
        # Check if we got any jobs
        if not jobs:
            error_msg = "No jobs were scraped"
            logger.error(error_msg)
            notify_failure(error_msg, "main")
        else:
            # Save jobs to file
            save_success = save_jobs_to_file(jobs, output_file)
            
            # Send success notification
            if DEFAULT_CHAT_ID:
                end_time = datetime.now()
                duration = end_time - start_time
                success_message = (
                    f"✅ Reed scraper completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"Duration: {duration.total_seconds():.1f} seconds\n"
                    f"Scraped {len(jobs)} jobs from {total_pages} pages"
                )
                send_message(TOKEN, success_message, DEFAULT_CHAT_ID)
                
            logger.info(f"✅ Total jobs scraped: {len(jobs)}")
            
    except Exception as e:
        error_msg = f"Unexpected error in main: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        notify_failure(error_msg, "main")
        sys.exit(1)
        
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"🏁 Reed scraper finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️ Total runtime: {duration.total_seconds():.1f} seconds")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Reed Job Scraper")
    parser.add_argument("--url", type=str, default=DEFAULT_BASE_URL, 
                        help="Base URL for Reed job search")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV_PATH,
                        help="Path to CSV file with company list")
    parser.add_argument("--output", type=str, default=OUTPUT_FILE,
                        help="Output JSON file path")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug mode with more verbose logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    main(args.url, args.csv, args.output)