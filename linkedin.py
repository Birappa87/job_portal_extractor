# linkedin_job_scraper.py
import asyncio
import json
import os
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# --------------- Config ------------------
MAX_CONCURRENT_DETAILS = 5
HEADLESS_MODE = False
SCROLL_PAUSE = 1.0
SCROLL_COUNT = 3
JOB_BACKUP_INTERVAL = 20

# --------------- Utils ------------------

def random_delay(min_sec=0.5, max_sec=1.5):
    return random.uniform(min_sec, max_sec)

async def dismiss_modal(page):
    selectors = [
        'button[aria-label="Dismiss"]',
        'button.artdeco-modal__dismiss',
        'button[aria-label="Close"]',
        '.artdeco-modal__dismiss',
        '.modal__dismiss',
        '.icon-close',
        '.sign-up-modal__dismiss'
    ]
    for sel in selectors:
        try:
            if await page.query_selector(sel):
                await page.click(sel, timeout=1000)
                await asyncio.sleep(random_delay())
                return True
        except:
            pass
    return False

async def scroll_page(page, scroll_count=SCROLL_COUNT):
    for _ in range(scroll_count):
        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.7)")
        await asyncio.sleep(SCROLL_PAUSE)

async def has_next_page(page):
    selectors = [
        'button.artdeco-pagination__button--next:not([disabled])',
        'li.artdeco-pagination__button--next:not(.artdeco-pagination__button--disabled)',
        '.pagination__control--next:not(.pagination__control--disabled)',
        'a[data-tracking-control-name="pagination-right"]'
    ]
    for sel in selectors:
        if await page.query_selector(sel):
            return True
    return False

async def click_next_page(page):
    selectors = [
        'button.artdeco-pagination__button--next:not([disabled])',
        'li.artdeco-pagination__button--next:not(.artdeco-pagination__button--disabled)',
        '.pagination__control--next:not(.pagination__control--disabled)',
        'a[data-tracking-control-name="pagination-right"]'
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await asyncio.sleep(random_delay(2.0, 3.0))
                return True
        except:
            continue
    return False

async def get_job_details(context, job_url, semaphore):
    job_details = {}
    async with semaphore:
        try:
            new_page = await context.new_page()
            await new_page.goto(job_url, timeout=15000)
            await asyncio.sleep(random_delay())
            await dismiss_modal(new_page)

            selectors = [
                '.show-more-less-html__markup', '.description__text',
                '.jobs-description__content', '.jobs-box__html-content'
            ]
            for sel in selectors:
                el = await new_page.query_selector(sel)
                if el:
                    job_details['description'] = (await el.inner_text()).strip()
                    break

            criteria_selectors = [
                '.description__job-criteria-item',
                '.jobs-description-details__list-item'
            ]
            for cs in criteria_selectors:
                items = await new_page.query_selector_all(cs)
                for item in items:
                    header = await item.query_selector('.description__job-criteria-subheader') or await item.query_selector('.jobs-description-details__list-item-label')
                    text = await item.query_selector('.description__job-criteria-text') or await item.query_selector('.jobs-description-details__list-item-value')

                    if header and text:
                        key = (await header.inner_text()).strip().lower().replace(' ', '_')
                        job_details[key] = (await text.inner_text()).strip()

            logo_el = await new_page.query_selector("img.org-top-card-primary-content__logo")
            job_details['company_logo'] = await logo_el.get_attribute("src") if logo_el else ""

            apply_el = await new_page.query_selector(".jobs-apply-button--top-card a")
            job_details['apply_link'] = await apply_el.get_attribute("href") if apply_el else job_url

            await new_page.close()
        except Exception as e:
            job_details['description'] = f"Error: {str(e)}"
    return job_details

# --------------- Main Scraper ------------------

async def scrape_linkedin_jobs(job_title="software engineer", location="United Kingdom"):
    async with async_playwright() as p:
        iphone_config = {
            'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
            'viewport': {'width': 390, 'height': 844},
            'device_scale_factor': 3,
            'is_mobile': True,
            'has_touch': True,
            'locale': 'en-GB',
            'ignore_https_errors': True
        }

        browser = await p.webkit.launch(headless=HEADLESS_MODE)
        context = await browser.new_context(**iphone_config)

        async def block_unwanted_requests(route, request):
            if request.resource_type in ["image", "media", "font"]:
                await route.abort()
            else:
                await route.continue_()

        page = await context.new_page()
        await page.route("**/*", block_unwanted_requests)

        encoded_title = job_title.replace(" ", "%20")
        encoded_location = location.replace(" ", "%20")
        url = f"https://www.linkedin.com/jobs/search?keywords={encoded_title}&location={encoded_location}&geoId=101165590&f_JT=F&f_SB2=42&f_TPR=r86400&f_E=4"

        await page.goto(url, timeout=15000)
        await asyncio.sleep(random_delay(2.0, 3.0))
        await dismiss_modal(page)

        job_results = []
        job_urls_seen = set()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DETAILS)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"linkedin_jobs_{timestamp}.json"
        os.makedirs("backup_data", exist_ok=True)

        jobs_scraped = 0
        page_number = 0

        while True:
            await scroll_page(page)
            job_cards = await page.query_selector_all('.job-search-card')

            print(f"[Page {page_number + 1}] Found {len(job_cards)} jobs")
            tasks = []

            for job_card in job_cards:
                try:
                    title_el = await job_card.query_selector('.base-search-card__title')
                    company_el = await job_card.query_selector('.base-search-card__subtitle')
                    location_el = await job_card.query_selector('.job-search-card__location')
                    link_el = await job_card.query_selector('a.base-card__full-link')

                    if not (title_el and company_el and location_el and link_el):
                        continue

                    title = (await title_el.inner_text()).strip()
                    company = (await company_el.inner_text()).strip()
                    location_text = (await location_el.inner_text()).strip()
                    job_url = await link_el.get_attribute('href')

                    if job_url in job_urls_seen:
                        continue

                    job_urls_seen.add(job_url)

                    job_data = {
                        'title': title,
                        'company': company,
                        'location': location_text,
                        'url': job_url,
                        'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    task = asyncio.create_task(get_job_details(context, job_url, semaphore))
                    task.job_data = job_data
                    tasks.append(task)

                except Exception as e:
                    print(f"Error parsing job card: {e}")
                    continue

            details = await asyncio.gather(*tasks)
            for i, detail in enumerate(details):
                data = tasks[i].job_data
                data.update(detail)
                job_results.append(data)
                jobs_scraped += 1

                if jobs_scraped % JOB_BACKUP_INTERVAL == 0:
                    backup_file = f"backup_data/backup_linkedin_jobs_{timestamp}_{jobs_scraped}.json"
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        json.dump(job_results, f, indent=4)
                    print(f"Backup saved: {backup_file}")

            if await has_next_page(page):
                await click_next_page(page)
                page_number += 1
                await asyncio.sleep(random_delay(2.0, 4.0))
            else:
                break

        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(job_results, f, indent=4)

        print(f"Scraped {jobs_scraped} jobs in {page_number + 1} pages. Saved to {json_filename}")
        await browser.close()

        return {
            'jobs_scraped': jobs_scraped,
            'pages_scraped': page_number + 1,
            'output_file': json_filename
        }

if __name__ == '__main__':
    result = asyncio.run(scrape_linkedin_jobs(job_title="data analyst", location="United Kingdom"))
    print("Summary:", result)
