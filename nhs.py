import requests
from bs4 import BeautifulSoup
import datetime
import json
import logging
import re
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.jobs.nhs.uk/candidate/search/results"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
PARAMS = {
    'searchFormType': 'main',
    'searchByLocationOnly': 'true',
    'language': 'en'
}
OUTPUT_FILE = "nhs_jobs.json"

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
        message = f"❌ NHS JOBS SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(stats):
    """Sends a success notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"✅ NHS JOBS SCRAPER SUCCESS at {timestamp}\n"
        message += f"Total jobs scraped: {stats['total']}\n"
        message += f"Matched companies: {stats['matched']}\n"
        message += f"Saved to: {OUTPUT_FILE}"
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
        logger.info(f"Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        error_message = f"Failed to load company list: {str(e)}"
        notify_failure(error_message, "get_company_list")
        raise

def is_matching_company(employer):
    """Checks if the employer matches any company in our target list."""
    try:
        clean_employer = clean_name(employer)
        for company in company_list:
            if company in clean_employer or clean_employer in company:
                return True
        return False
    except Exception as e:
        error_message = f"Failed to check company match: {str(e)}"
        notify_failure(error_message, "is_matching_company")
        return False

def scrape_all_pages():
    """Scrape all pages of job listings from the NHS jobs website."""
    all_jobs = []
    page = 1
    
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
            page += 1
            
        except requests.RequestException as e:
            error_message = f"Error fetching page {page}: {str(e)}"
            logger.error(error_message)
            notify_failure(error_message, f"scrape_all_pages (page {page})")
            break
    
    logger.info(f"Total jobs scraped: {len(all_jobs)}")
    return all_jobs

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
            is_target_company = is_matching_company(employer)
            
            location_element = job.find('div', class_='location-font-size')
            location = location_element.text.strip() if location_element else "Unknown Location"
            
            salary_element = job.find('li', {'data-test': 'search-result-salary'})
            salary = "Not specified"
            if salary_element:
                salary = salary_element.text.strip().replace('Salary:', '').strip()
                salary = salary.split('a year')[0].strip()
            
            closing_date_str = "Not specified"
            closing_date_element = job.find('li', {'data-test': 'search-result-closingDate'})
            days_until_closing = None
            
            if closing_date_element:
                closing_date_str = closing_date_element.text.strip().replace('Closing date:', '').strip()
                try:
                    closing_date = datetime.strptime(closing_date_str, '%d %B %Y').date()
                    closing_date_str = closing_date.strftime('%d/%m/%Y')
                    days_until_closing = (closing_date - current_date).days
                except ValueError:
                    closing_date_str = "Invalid date format"
            
            posting_date_str = "Not specified"
            posting_date_element = job.find('li', {'data-test': 'search-result-publicationDate'})
            
            if posting_date_element:
                posting_date_str = posting_date_element.text.strip().replace('Date posted:', '').strip()
                try:
                    posting_date = datetime.strptime(posting_date_str, '%d %B %Y').date()
                    posting_date_str = posting_date.strftime('%d/%m/%Y')
                except ValueError:
                    posting_date_str = "Invalid date format"
            
            job_id = None
            job_type = "Not specified"
            contract_type = "Not specified"
            
            job_id_element = job.find('span', {'data-test': 'search-result-jobId'})
            if job_id_element:
                job_id = job_id_element.text.strip().replace('Job reference:', '').strip()
            
            job_type_element = job.find('li', {'data-test': 'search-result-jobType'})
            if job_type_element:
                job_type = job_type_element.text.strip().replace('Job type:', '').strip()
            
            contract_type_element = job.find('li', {'data-test': 'search-result-contractType'})
            if contract_type_element:
                contract_type = contract_type_element.text.strip().replace('Contract type:', '').strip()
            
            jobs.append({
                'title': title,
                'url': url,
                'employer': employer,
                'location': location,
                'salary': salary,
                'closing_date': closing_date_str,
                'days_until_closing': days_until_closing,
                'posting_date': posting_date_str,
                'job_id': job_id,
                'job_type': job_type,
                'contract_type': contract_type,
                'is_target_company': is_target_company,
                'scraped_date': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            })
            
        except Exception as e:
            error_message = f"Error parsing job: {str(e)}"
            logger.error(error_message)
            notify_failure(error_message, "parse_jobs")
    
    return jobs

def save_to_json(jobs):
    """Save the scraped jobs to a JSON file."""
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=4, ensure_ascii=False)
        logger.info(f"Successfully saved {len(jobs)} jobs to {OUTPUT_FILE}")
        return True
    except Exception as e:
        error_message = f"Error saving jobs to JSON file: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "save_to_json")
        return False

def main():
    """Main function to run the scraper."""
    global chat_id
    logger.info("Starting NHS job scraper...")
    
    try:
        # Get Telegram chat ID
        chat_id = get_chat_id(TOKEN)
        
        # Load company list
        get_company_list()
        
        # Scrape all job listings
        jobs = scrape_all_pages()
        
        # Track matched companies
        matched_jobs = [job for job in jobs if job.get('is_target_company', False)]
        
        # Save the results to a JSON file
        if jobs:
            save_to_json(jobs)
            
            # Send success notification
            stats = {
                'total': len(jobs),
                'matched': len(matched_jobs)
            }
            notify_success(stats)
            
            # Print a sample of the scraped jobs
            logger.info("\nSample of scraped jobs:")
            for job in jobs[:3]:  # Print only the first 3 jobs as a sample
                logger.info(f"Title: {job['title']}")
                logger.info(f"Employer: {job['employer']}")
                logger.info(f"Location: {job['location']}")
                logger.info(f"Salary: {job['salary']}")
                logger.info(f"Closing Date: {job['closing_date']}")
                logger.info(f"Target Company: {'Yes' if job.get('is_target_company', False) else 'No'}")
                logger.info("---")
                
            logger.info(f"\nTotal jobs scraped: {len(jobs)}")
            logger.info(f"Matched companies: {len(matched_jobs)}")
        else:
            logger.info("No jobs were found.")
            notify_failure("No jobs were found.", "main")
            
    except Exception as e:
        error_message = f"Critical error in main function: {str(e)}"
        logger.error(error_message)
        notify_failure(error_message, "main")

if __name__ == "__main__":
    main()