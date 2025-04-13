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

            with open("glassdoor_all_uk.json", "w+", encoding="utf-8") as final_file:
                json.dump(all_data, final_file, indent=2)

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

            if chat_id:
                success_message = f"✅ Glassdoor scraper completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nScraped {len(all_data)} jobs across {len(urls)} UK regions"
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