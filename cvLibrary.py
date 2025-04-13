import json
import concurrent.futures
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
import time
import threading
import pandas as pd
from datetime import datetime
import os
import traceback
import requests

thread_local = threading.local()

company_list = []

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

def get_browser():
    """Get a thread-local browser instance"""
    try:
        if not hasattr(thread_local, "playwright"):
            thread_local.playwright = sync_playwright().start()
            thread_local.browser = thread_local.playwright.chromium.launch(headless=True)
        return thread_local.browser
    except Exception as e:
        error_message = f"Failed to initialize browser: {str(e)}"
        notify_failure(error_message, "get_browser")
        raise

def get_total_jobs():
    """Get the total number of jobs available on the site"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            page.goto("https://www.cv-library.co.uk/jobs-in-uk", wait_until="domcontentloaded")
            page.wait_for_selector("div.search-nav-actions__left p", timeout=10000)
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            total_jobs_text = soup.select_one("div.search-nav-actions__left p").get_text()
            match = re.search(r"Search ([\d,]+) jobs", total_jobs_text)
            
            context.close()
            browser.close()
            
            if match:
                return int(match.group(1).replace(",", ""))
            return 0
    except Exception as e:
        error_message = f"Failed to get total jobs: {str(e)}"
        notify_failure(error_message, "get_total_jobs")
        return 0

def scrape_page(page_num):
    """Scrape a single page of job listings using a thread-local browser"""
    jobs_on_page = []
    try:
        browser = get_browser()
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        url = f'https://www.cv-library.co.uk/jobs-in-uk?perpage=100&page={page_num}'
        print(f"Scraping page {page_num}...")
        
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_selector("ol#searchResults", timeout=10000)
        content = page.content()

        context.close()
        
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
                job_url = f'https://www.cv-library.co.uk/job/{job_id}' if job_id else ""
                industry = section_element.get("data-job-industry", "")

                company_logo_element = job.select_one("img.job__logo")
                company_logo = company_logo_element.get("data-src", "") if company_logo_element else ""

                description_tag = soup.select_one('p.job__description')
                description = description_tag.get_text(strip=True) if description_tag else ""

                apply_tag = soup.select_one('a.cvl-btn[href*="/apply"]')
                apply_link = apply_tag['href'] if apply_tag else ""

                # If the apply link is relative, prepend the base URL
                base_url = "https://www.cv-library.co.uk"
                if apply_link and apply_link.startswith("/"):
                    apply_link = base_url + apply_link

                ingestion_time = datetime.utcnow().isoformat()

                if company in company_list:
                    jobs_on_page.append({
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
                        "apply_link": apply_link,
                        "country": "UK",
                        "source": "cv_library",
                        "ingestion_timestamp": ingestion_time
                    })

            except Exception as e:
                print(f"Error parsing job on page {page_num}: {e}")
                # We don't notify for individual job parsing failures to avoid notification spam
    
    except Exception as e:
        error_message = f"Error scraping page {page_num}: {str(e)}"
        print(error_message)
        notify_failure(error_message, f"scrape_page({page_num})")
    
    return jobs_on_page

def cleanup_resources():
    """Clean up thread-local browser resources"""
    try:
        if hasattr(thread_local, "browser"):
            thread_local.browser.close()
        if hasattr(thread_local, "playwright"):
            thread_local.playwright.stop()
    except Exception as e:
        error_message = f"Failed to clean up resources: {str(e)}"
        notify_failure(error_message, "cleanup_resources")

def get_job_listings(max_workers=5, max_pages=None):
    """Get job listings using parallel processing"""
    job_list = []
    start_time = time.time()

    try:
        total_jobs = get_total_jobs()
        print(f"Total jobs found: {total_jobs}")
        
        if total_jobs == 0:
            notify_failure("No jobs found or failed to retrieve total job count", "get_job_listings")
            return []
            
        total_pages = (total_jobs // 100) + (1 if total_jobs % 100 != 0 else 0)
        
        if max_pages and max_pages < total_pages:
            total_pages = max_pages
            print(f"Limiting to {max_pages} pages")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {
                executor.submit(scrape_page, page_num): page_num 
                for page_num in range(1, total_pages + 1)
            }
            
            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    page_results = future.result()
                    job_list.extend(page_results)
                    print(f"Page {page_num} completed with {len(page_results)} jobs")

                except Exception as e:
                    error_message = f"Page {page_num} generated an exception: {str(e)}"
                    print(error_message)
                    notify_failure(error_message, f"future_processing(page={page_num})")

        cleanup_resources()
        
        elapsed_time = time.time() - start_time
        print(f"Scraping completed in {elapsed_time:.2f} seconds")
        print(f"Total jobs scraped: {len(job_list)}")
        
        # Notify if we got very few jobs (potential failure)
        if len(job_list) < 10 and total_jobs > 10:
            notify_failure(f"Only {len(job_list)} jobs scraped out of {total_jobs} total jobs", "get_job_listings")
            
        return job_list
        
    except Exception as e:
        error_message = f"Failed in get_job_listings: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        notify_failure(error_message, "get_job_listings")
        return []

if __name__ == "__main__":
    try:
        # Initialize chat_id at startup
        chat_id = get_chat_id(TOKEN)
        
        # Send startup notification
        if chat_id:
            send_message(TOKEN, f"🚀 CV Library scraper started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", chat_id)
        
        get_company_list()
        job_listings = get_job_listings(max_workers=30, max_pages=None)
        
        if not job_listings:
            notify_failure("No job listings returned from scraper", "main")
            exit(1)
            
        df = pd.DataFrame(job_listings)
        
        # Select and order columns
        columns = [
                "title", "experience", "salary", "location", "job_type",
                "url", "company", "description", "posted_date", "company_logo", 
                "apply_link", "country", "source", "ingestion_timestamp"
            ]
        
        # Filter to include only columns that exist in the DataFrame
        existing_columns = [col for col in columns if col in df.columns]
        df = df[existing_columns]

        # Save data
        try:

            os.makedirs('data', exist_ok=True)
            df.to_csv('data/sample_data.csv', index=False)
            
            # Send success notification
            if chat_id:
                success_message = f"✅ CV Library scraper completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nScraped {len(job_listings)} jobs"
                send_message(TOKEN, success_message, chat_id)
                
        except Exception as e:
            error_message = f"Failed to save data: {str(e)}"
            notify_failure(error_message, "data_saving")
            
    except Exception as e:
        error_message = f"Critical failure in main: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        notify_failure(error_message, "main")