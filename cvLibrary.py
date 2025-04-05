import json
import concurrent.futures
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
import time
import threading
import pandas as pd

thread_local = threading.local()

company_list = []

def get_company_list():
    global company_list
    df = pd.read_csv(r"C:\Users\birap\Downloads\2025-04-04_-_Worker_and_Temporary_Worker.csv")
    company_list = list(df['Organisation Name'])

def get_browser():
    """Get a thread-local browser instance"""
    if not hasattr(thread_local, "playwright"):
        thread_local.playwright = sync_playwright().start()
        thread_local.browser = thread_local.playwright.chromium.launch(headless=True)
    return thread_local.browser

def get_total_jobs():
    """Get the total number of jobs available on the site"""
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
                title = section_element.get("data-job-title", "")
                company = section_element.get("data-company-name", "")
                location = section_element.get("data-job-location", "")
                salary = section_element.get("data-job-salary", "")
                job_type = section_element.get("data-job-type", "")
                date_posted = section_element.get("data-job-posted", "")
                job_id = section_element.get("data-job-id", "")
                job_url = f'https://www.cv-library.co.uk/job/{job_id}' if job_id else ""

                company_logo_element = job.select_one("img.job__logo")
                company_logo = company_logo_element.get("src", "") if company_logo_element else ""

                if company in company_list:
                    jobs_on_page.append({
                                    "Title": title,
                                    "Company": company,
                                    "Location": location,
                                    "Salary": salary,
                                    "Job Type": job_type,
                                    "URL": job_url,
                                    "Date Posted": date_posted,
                                    "Company Logo": company_logo
                                })
            except Exception as e:
                print(f"Error parsing job on page {page_num}: {e}")
    
    except Exception as e:
        print(f"Error scraping page {page_num}: {e}")
    
    return jobs_on_page

def cleanup_resources():
    """Clean up thread-local browser resources"""
    if hasattr(thread_local, "browser"):
        thread_local.browser.close()
    if hasattr(thread_local, "playwright"):
        thread_local.playwright.stop()

def get_job_listings(max_workers=5, max_pages=None):
    """Get job listings using parallel processing"""
    job_list = []
    start_time = time.time()

    total_jobs = get_total_jobs()
    print(f"Total jobs found: {total_jobs}")
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
                print(f"Page {page_num} generated an exception: {e}")

    cleanup_resources()
    
    elapsed_time = time.time() - start_time
    print(f"Scraping completed in {elapsed_time:.2f} seconds")
    print(f"Total jobs scraped: {len(job_list)}")
    
    return job_list

if __name__ == "__main__":
    get_company_list()
    job_listings = get_job_listings(max_workers=30, max_pages=None)
    
    with open("jobs.json", "w+", encoding="utf-8") as file:
        json.dump(job_listings, file, indent=4)

    print(f"Sample of first 3 jobs:")
    print(json.dumps(job_listings[:3], indent=4))