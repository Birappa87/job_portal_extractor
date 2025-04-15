# linkedin_job_scraper.py
from playwright.sync_api import sync_playwright
from datetime import datetime
import time
import json


def get_chrome_options():
    return {
        "viewport": {
            "width": 390,
            "height": 844
        },
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "device_scale_factor": 3,
        "is_mobile": True,
        "has_touch": True,
        "locale": "en-GB",
    }


def dismiss_popup_if_exists(page):
    try:
        dismiss_button = page.query_selector("button[data-tracking-control-name='public_jobs_contextual-sign-in-modal_modal_dismiss']")
        if dismiss_button:
            dismiss_button.click()
            time.sleep(0.5)
    except Exception:
        pass


def scrape_jobs_from_linkedin(page):
    jobs_data = []
    while True:
        dismiss_popup_if_exists(page)

        job_cards = page.query_selector_all(".jobs-search-results__list-item")

        for card in job_cards:
            title = card.query_selector(".base-search-card__title")
            title = title.inner_text().strip() if title else ""

            company = card.query_selector(".base-search-card__subtitle")
            company = company.inner_text().strip() if company else ""

            location = card.query_selector(".job-search-card__location")
            location = location.inner_text().strip() if location else ""

            job_url = card.query_selector("a")
            job_url = job_url.get_attribute("href") if job_url else ""

            posted_date = card.query_selector("time")
            posted_date = posted_date.get_attribute("datetime") if posted_date else ""

            if job_url:
                with page.expect_navigation():
                    page.goto(job_url)
                time.sleep(1)
                dismiss_popup_if_exists(page)

                description_el = page.query_selector(".show-more-less-html__markup")
                description = description_el.inner_text().strip() if description_el else ""

                job_type = ""
                salary = ""
                region = ""
                company_logo_el = page.query_selector("img.org-top-card-primary-content__logo")
                company_logo = company_logo_el.get_attribute("src") if company_logo_el else ""

                external_apply_link_el = page.query_selector(".jobs-apply-button--top-card a")
                apply_link = external_apply_link_el.get_attribute("href") if external_apply_link_el else job_url

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
                    "posted_date": posted_date,
                    "company_logo": company_logo,
                    "apply_link": apply_link,
                    "country": "UK",
                    "source": "glassdoor",
                    "ingestion_timestamp": ingestion_time,
                    "region": region
                }

                jobs_data.append(job_data)
                page.go_back()
                time.sleep(1)
                dismiss_popup_if_exists(page)

        next_btn = page.query_selector("button[aria-label='Next']")
        if next_btn and not next_btn.is_disabled():
            next_btn.click()
            time.sleep(2)
        else:
            break

    return jobs_data


def main():
    with sync_playwright() as p:
        iphone = p.webkit.launch(headless=False)
        context = iphone.new_context(**get_chrome_options())
        page = context.new_page()

        page.goto("https://www.linkedin.com/jobs/search?keywords=&location=United%20Kingdom")
        time.sleep(3)
        dismiss_popup_if_exists(page)

        all_jobs = scrape_jobs_from_linkedin(page)

        with open("linkedin_jobs.json", "w", encoding="utf-8") as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)

        iphone.close()


if __name__ == "__main__":
    main()
