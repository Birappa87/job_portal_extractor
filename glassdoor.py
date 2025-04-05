from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import random
import json
import pandas as pd
import re

company_list = []

def clean_name(name):
    name = str(name).strip().lower()
    name = re.sub(r'[^a-z0-9\s]', '', name)  # Remove special characters
    name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with single space
    return name

def get_company_list():
    global company_list
    df = pd.read_csv(r"C:\Users\birap\Downloads\2025-04-04_-_Worker_and_Temporary_Worker.csv")
    df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
    company_list = list(df['Organisation Name'])

def extract_data(content):
    soup = BeautifulSoup(content, 'html.parser')
    job_element = soup.find('ul', class_='JobsList_jobsList__lqjTr')
    if not job_element:
        print("No job list found.")
        return []

    job_cards = job_element.find_all('li')
    print(f"Total Jobs Extracted: {len(job_cards)}\n")

    all_jobs = []

    for job in job_cards:
        try:
            title_element = job.find('a', class_='JobCard_jobTitle__GLyJ1')
            title = title_element.get_text(strip=True)
            job_url = title_element['href']
            if not job_url.startswith("http"):
                job_url = f"https://www.glassdoor.co.uk{job_url}"

            company_element = job.find('span', class_='EmployerProfile_compactEmployerName__9MGcV')
            company = company_element.get_text(strip=True) if company_element else "N/A"

            location_element = job.find('div', class_='JobCard_location__Ds1fM')
            location = location_element.get_text(strip=True) if location_element else "N/A"

            salary_element = job.find('div', class_='JobCard_salaryEstimate__QpbTW')
            salary = salary_element.get_text(strip=True) if salary_element else "N/A"

            date_posted_element = job.find('div', class_='JobCard_listingAge__jJsuc')
            date_posted = date_posted_element.get_text(strip=True) if date_posted_element else "N/A"

            job_type = "Easy Apply" if job.find('div', class_='JobCard_easyApplyTag__5vlo5') else "Standard"

            logo_img = job.find('div', class_='EmployerProfile_profileContainer__63w3R')
            company_logo = logo_img.find('img')['src'] if logo_img and logo_img.find('img') else "N/A"

            if company.lower() in company_list:
                job_data = {
                    "Title": title,
                    "Company": company,
                    "Location": location,
                    "Salary": salary,
                    "Job Type": job_type,
                    "URL": job_url,
                    "Date Posted": date_posted,
                    "Company Logo": company_logo
                }

                all_jobs.append(job_data)
        except Exception as e:
            print(f"Skipping job due to error: {e}")
            continue

    print(f"✅ Total jobs collected: {len(all_jobs)}\n")
    return all_jobs

urls = {
    "england": "https://www.glassdoor.co.uk/Job/england-uk-jobs-SRCH_IL.0,10_IS7287.htm?maxSalary=9000000&minSalary=250000",
    "scotland": "https://www.glassdoor.co.uk/Job/scotland-uk-jobs-SRCH_IL.0,11_IS7289.htm?maxSalary=9000000&minSalary=250000",
    "wales": "https://www.glassdoor.co.uk/Job/wales-uk-jobs-SRCH_IL.0,8_IS7290.htm?maxSalary=9000000&minSalary=250000",
    "northern_ireland": "https://www.glassdoor.co.uk/Job/northern-ireland-uk-jobs-SRCH_IL.0,19_IS7288.htm?maxSalary=9000000&minSalary=250000"
}

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        locale="en-GB,en;q=0.5",
        geolocation={"latitude": 51.509865, "longitude": -0.118092},
        permissions=["geolocation"],
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    page = context.new_page()

    all_data = []

    for region, url in urls.items():
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
            except:
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
            except:
                pass

            if current_count == prev_count:
                stable_rounds += 1
                if stable_rounds >= 3:
                    print("No new jobs loaded after several attempts.")
                    break
            else:
                stable_rounds = 0
                prev_count = current_count

        final_content = page.content()
        region_jobs = extract_data(final_content)
        all_data.extend(region_jobs)

    with open("glassdoor_all_uk.json", "w+", encoding="utf-8") as final_file:
        json.dump(all_data, final_file, indent=2)

    browser.close()