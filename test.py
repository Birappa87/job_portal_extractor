from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import random

def extract_data(content):
    soup = BeautifulSoup(content, 'html.parser')

    job_element = soup.find('ul', class_='JobsList_jobsList__lqjTr')
    if not job_element:
        print("No job list found.")
        return

    job_cards = job_element.find_all('li')

    for job in job_cards:
        try:
            title_element = job.find('a', class_='JobCard_jobTitle__GLyJ1')
            title = title_element.get_text(strip=True)
            print(title)
        except Exception as e:
            continue

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    page = context.new_page()
    
    page.goto("https://www.glassdoor.co.in/Job/united-kingdom-jobs-SRCH_IL.0,14_IN2.htm", wait_until="domcontentloaded")

    # Keep clicking "Show more jobs" button as long as it appears
    while True:
        try:
            page.wait_for_selector("button.CloseButton", timeout=3000)
            close_button = page.query_selector("button.CloseButton")
            if close_button:
                print("Popup detected. Closing it.")
                close_button.click()
                time.sleep(1)
        except:
            pass

        try:
            page.wait_for_selector('button[data-test="load-more"]', timeout=6000)
            button = page.query_selector('button[data-test="load-more"]')
            if button:
                print("Clicking 'Show more jobs' button...")
                button.click()
                sleep_duration = random.uniform(1, 5)
                print(f"Sleeping for {sleep_duration:.2f} seconds...")
                time.sleep(sleep_duration)
            else:
                break
        except:
            print("No more 'Show more jobs' button.")
            break

    # Final content after all jobs are loaded
    content = page.content()

    with open("glassdoor.html", "w+", encoding="utf-8") as file:
        file.write(content)

    extract_data(content)

    browser.close()
