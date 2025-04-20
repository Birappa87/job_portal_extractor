import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
import re
import json
import time

# Telegram Bot Setup
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
company_list = []

def get_chat_id(TOKEN: str) -> str:
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        response = requests.get(url)
        info = response.json()
        return info['result'][0]['message']['chat']['id']
    except Exception as e:
        print(f"Failed to get chat ID: {e}")
        return None

def send_message(TOKEN: str, message: str, chat_id: str):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {'chat_id': chat_id, 'text': message}
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("✅ Telegram message sent.")
        else:
            print(f"⚠️ Telegram error: {response.status_code}")
    except Exception as e:
        print(f"Failed to send message: {e}")

def notify_failure(error_message, location="Unknown"):
    global chat_id
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ TOTALJOBS SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(message):
    global chat_id
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        send_message(TOKEN, message, chat_id)

# Cleaning and matching
def clean_name(name):
    try:
        name = str(name).strip().lower()
        name = re.sub(r'[^a-z0-9\s]', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name
    except Exception as e:
        notify_failure(f"Failed to clean name: {str(e)}", "clean_name")
        return ""

def get_company_list():
    global company_list
    try:
        df = pd.read_csv(r"C:\\Users\\birap\\Downloads\\2025-04-04_-_Worker_and_Temporary_Worker.csv")
        df['Organisation Name'] = df['Organisation Name'].apply(clean_name)
        company_list = list(df['Organisation Name'])
        print(f"✅ Loaded {len(company_list)} companies from CSV")
    except Exception as e:
        notify_failure(f"Failed to load company list: {str(e)}", "get_company_list")
        raise

# Scraper logic
def fetch_totaljobs_page(page_num: int) -> str:
    cookies = {
        'VISITOR_ID': '1eae292b5e9dd40ea7de23ed3c7588bf',
        'anonymousUserId': 'a8253410-fc24-ca72-66cc-749150c81d7e',
        'sc_vid': '55e69705c98e379955e8f089a60d2087',
        'AnonymousUser': 'MemberId=23fe22bc-1da5-41ed-9857-2fba4b2fc6ff&IsAnonymous=True',
        'optimizelySession': '1744560576239',
        '_hjSessionUser_2060791': 'eyJpZCI6IjhhNDUyY2Y3LTZhMDMtNWU1Yi1iMzkyLTFmNGE2NjI5ZGM2MyIsImNyZWF0ZWQiOjE3NDQ1NjA1ODMyMjYsImV4aXN0aW5nIjp0cnVlfQ==',
        '_gcl_au': '1.1.1333283508.1744560583',
        '_fbp': 'fb.1.1744560583568.36711758334213624',
        '_ga': 'GA1.1.1987848688.1744560604',
        'optimizelySession': '0',
        'STEPSTONEV5LANG': 'en',
        'listing_page__qualtrics': 'run',
        'SessionCookie': 'a48a811b-92ba-4d8e-9d11-8066ab9f1392',
        'SsaSessionCookie': 'f3dfb1eb-2cce-4647-9b9c-c9143d17f7d9',
        '_abck': '8F2696F8E81ADF02014C742FFCEBCE1D~0~YAAQTzkgFzTCbEmWAQAA8mCOUQ3LEbGKFBKRt93K9ibg+kHs2kKIBnyf3wQX/JDSgmiasnVizMVwDy41VydJ7p10YzT5U4Jullw8XuXghd2LVJINmPT1myGEZ1eriZysgPkhV/yfeC5iz6t5aFldKODSSytU1/p2n4zdtlvElHJimcKwqv+Dgj3JnJ3P4gf7YlzCjI4dvB+oYeLfpHEI1yd525iiG320wv20b0UwbeYTR5yD2laQxk91SB6vp5S9dBWRFPF5oorwv23jaCr8aFuSefJIosVH+99VdnIXx/VBQFIVOEmxLBXrfV6syXY+gOqw6h9BA6O4o3tDnabAaDRPJateyNVx9SOI4XaCUOYX9dkmH+XuwQSoSS60q948vtrLdpYzJyGcQYRNFdmvCp/kYKsZ95wQHb6jqd9lI0ZgpxoLgCEs80MpkohiP5e2EVpi/ixh0fuQavkHI+QTnGp1qG1ohuw2F3nIMOV+O8/Evx1tiOW1YGPIep/IlQ7tAYuTIMswzcEzAFfJNdRaHFgvGyA=~-1~-1~-1',
        '_hjSession_2060791': 'eyJpZCI6ImZlMWQ1MzdkLTBlMDUtNGViMi1hMjQ5LWVmNGE3YjYyMGExMSIsImMiOjE3NDUxMjUwMTAzODYsInMiOjEsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=',
        '_clck': 'jv7gqb%7C2%7Cfv8%7C0%7C1929',
        'ak_bmsc': '5862757E7A36DE46D4453FA3DE3AD2AE~000000000000000000000000000000~YAAQTzkgF1TCbEmWAQAAZ6GOURuAEsXKf2xVv/kCiaHt4OQzsttjfK/095t7/Ym2uYoIwGngJ5Z/W5PqgG/rOuZ8P4VDTT4Ee9Sp7/Ys6AHbTTc8vpW8tj32G0KGOqp0WgZGGrXugesfnDi/zu2C3pvdRRj6bAGGWfMKQrjMtdyHBlUUlwY4f5z+0bmAJALgSBrQJoOvCcHWGK+6FFBcPXqLRY01IP7HAlrQSWktewel1vFZ6bzK58ah/cx1fs0UTYj6gpLjg9vcW4z2a9bL6XH6L12txTkBuO8ATtUehQhFkfTg0kyhYVeXrDSa3TM4y3cgR+MTIshilGWwDTvRpUtws3AfD6g4nlQYxaKEauwIgMz8RTNrlzuAbW6RAnngbifnxbcs6bDf3at94gzOEpH492k/0y7n4QpqDk9TWbrkM0dxYS67IfAfy8VKvzqdtuBQ8FYIkdigIC3W1nMtPzD1NktOhCw=',
        'bm_sv': '7A4A1F4237060A124D75AEB2ECC0FAFC~YAAQTzkgFwbEbEmWAQAAtwmRURvtfjkXPHkxl+weEnEyteiLjqaMUzxBCfIZi7H2N4foJ/GGAjjkSfuFjjawqEJGBfp99sIk11eanDTdpX8IICs7IZnOXKoa33zzb8pf+HpNOmyi8AeQJGHOKFlZepcz9lovUw0AGSoD5aPPIUpZBj0fbrR1THhsNVeN2Kh993V3xx9mq/0kXx0R7eWsImAdze83jhZ+Kgt7JCnC/Qh5EM9QbxanNhtGKTqt+hykGeUQ~1',
        'bm_sz': 'A260A44BB84D36D76D008E790E6A8A4C~YAAQTzkgF1HEbEmWAQAA/UmRURvNgn0WgrbPixkY+hH2hAPxDIyP1GXp6xqBELEpd4M4MBzvN8cG2QATjeqzKCKKcplUGe1L3T4ocm4L5IJBvnbEf7pFM8Vyq9meUjXmW+z02P5cH+J4TrTRTHFmnxgsFXSWg8JRZmdOOXiq8Fb+SCILjHQzHJtxImIvXcsc5c2ZfJ2kmfvIerZuSblA9t5IsWCCoUwoMfi3cWCTQLtH8wEATOgrqxFjc9lXIydUyK1HFsqXeU7eP/Vxpkn+zPMrzAZabwX+xBnJ4gkPD7y7cvqbWN51HaxVsHF9cjB3YuTJ0MU8YRWLKn62qHTk/EVoPna3IO7VoOrWK9G3srfxJ+7o/blkI6pfRnARxQ2DPPW2Aj29TpvCTncJSkekDqc/VgBb1PLmN19vzAKW0F5TIw==~4470339~4534854',
        'EntryUrl': '/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1&action=facet_selected%3bsalary%3b30000%3bsalarytypeid%3b1',
        'SearchResults': '"104740055,104707469,104695324,104678831,104601614,104521678,104695119,104468177,104561633,104742194,104737809,104737808,104722229,104634770,104634782,104634780,104634775,104634772,104629792,104676660,104676621,104676604,104676579,104568931,104655837"',
        '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%2223fe22bc-1da5-41ed-9857-2fba4b2fc6ff%22%2C%22expiryDate%22%3A%222026-04-20T05%3A00%3A02.366Z%22%7D',
        '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22pwU7WZAJpJ1yjKLsMh5z%22%2C%22expiryDate%22%3A%222026-04-20T05%3A00%3A02.366Z%22%7D',
        'cto_bundle': 'JoNcRV81Y3MlMkIlMkJwNElISjE0S0VtR1BsRUZtTzZyWVk1dG5OciUyRk5veEozUVJBRFNzYjlXWEtkQlN2ekN1ckMwYXFtemxOYU42SVhPYW84ZERnc1dJTFl2eVdTU3c0WVVrVVB3OExIdWdUNjVYOXE5cmtRUWRPa3hScCUyRnNUbnNUVHFHQTZ3b3dQRFlZWFBibEp6RUtEdmVVeDRnOU1WUWNITyUyQnF3djVsalRXV0l1aHlGcWFoWUxxUUxtNko2a0hia0hXYlpCb1ZoWmszODI0eEcyaktCSFolMkZBZGRidTdtdzA5MkdZdFlvRHZsV2VqSmxnWW83ck04SVlDclRLTUVUTTZYOSUyQkg1b0N0SzZocVZXTVlmcENjcTg5STJ3JTNEJTNE',
        '_uetsid': 'c921bf301c8411f0b0d595f8012f6c1c',
        '_uetvid': 'b5fa0900188111f0af7a07bdcdc07c19',
        '_ga_F6MR9F9R5K': 'GS1.1.1745125007164.6.1.1745125202.0.0.0',
        '_clsk': 'mpxsbl%7C1745125203099%7C4%7C1%7Ci.clarity.ms%2Fcollect',
        'QSI_HistorySession': 'https%3A%2F%2Fwww.totaljobs.com%2Fjobs%2Fsponsorship%2Fin-united-kingdom~1745125012710%7Chttps%3A%2F%2Fwww.totaljobs.com%2Fjobs%2Fsponsorship%2Fin-united-kingdom%3Fsalary%3D30000%26salarytypeid%3D1%26action%3Dfacet_selected%253bsalary%253b30000%253bsalarytypeid%253b1~1745125203771',
        '_dd_s': 'logs=1&id=9abc737e-57ab-4d1c-9b60-856f684f9468&created=1745125008721&expire=1745126108700',
        'CONSENTMGR': 'c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:0%7Cc6:1%7Cc7:1%7Cc8:0%7Cc9:1%7Cc10:0%7Cc11:0%7Cc12:1%7Cc13:1%7Cc14:0%7Cc15:0%7Cts:1745125209011%7Cconsent:true',
        'utag_main': 'v_id:01962fe9d70a00a023828eee45d00506f001a06700978$_sn:4$_se:11$_ss:0$_st:1745127009012$dc_visit:4$ses_id:1745125007164%3Bexp-session$_pn:4%3Bexp-session$PersistedFreshUserValue:0.1%3Bexp-session$prev_p:%3Bexp-session$dc_event:7%3Bexp-session$dc_region:eu-central-1%3Bexp-session',
        'RT': '"z=1&dm=www.totaljobs.com&si=dbcd41f2-e862-4d01-8d0e-e86eeddee462&ss=m9p6dbzs&sl=4&tt=fa5&obo=1&rl=1&nu=1gziv30xh&cl=4d6d&ld=4dbi&r=ygk6r0gy&ul=4dbi"',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7,kn;q=0.6',
        'priority': 'u=0, i',
        'referer': 'https://www.totaljobs.com/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1&action=facet_selected%3bsalary%3b30000%3bsalarytypeid%3b1',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36',
    }

    response = requests.get(
        f'https://www.totaljobs.com/jobs/sponsorship/in-united-kingdom?salary=30000&salarytypeid=1&page={page_num}&action=facet_selected%3bsalary%3b30000%3bsalarytypeid%3b1',
        cookies=cookies,
        headers=headers,
    )

    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to fetch page {page_num}, status code: {response.status_code}")

def extract_job_data(html: str, page_num: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, 'html.parser')
    job_elements = soup.select('article.res-sfoyn7')

    jobs = []
    for job in job_elements:
        try:
            title = job.select_one('div.res-nehv70')
            company = job.select_one('span.res-btchsq')
            location = job.select_one('[data-at="job-item-location"] span.res-btchsq')
            salary = job.select_one('[data-at="job-item-salary-info"]')
            job_url = job.select_one('a.res-1foik6i')
            description = job.select_one('div.res-1d1eotm')
            date_posted = job.select_one('time')
            company_logo = job.select_one('img.res-1cyaovz')
            labels = job.select('[data-at="job-item-top-label"]')
            job_type = job.select_one('[data-at="job-item-work-type"]')

            full_url = "https://www.totaljobs.com" + job_url['href'] if job_url and job_url.has_attr('href') else "N/A"
            job_id = full_url.split('/')[-1].split('?')[0] if full_url != "N/A" else "N/A"
            raw_company = company.get_text(strip=True) if company else "N/A"

            if clean_name(raw_company) not in company_list:
                continue

            job_data = {
                "job_id": job_id,
                "title": title.get_text(strip=True) if title else "N/A",
                "experience": "",
                "salary": salary.get_text(strip=True) if salary else "N/A",
                "location": location.get_text(strip=True) if location else "N/A",
                "job_type": job_type.get_text(strip=True) if job_type else "",
                "url": full_url,
                "company": raw_company,
                "description": description.get_text(strip=True) if description else "N/A",
                "posted_date": date_posted.get_text(strip=True) if date_posted else "N/A",
                "company_logo": company_logo['src'] if company_logo and company_logo.has_attr('src') else "N/A",
                "apply_link": full_url,
                "country": "UK",
                "source": "totaljobs",
                "labels": [label.get_text(strip=True) for label in labels] if labels else [],
                "page_number": page_num,
                "ingestion_timestamp": datetime.utcnow().isoformat()
            }

            jobs.append(job_data)

        except Exception as e:
            notify_failure(f"Job extraction error: {e}", "extract_job_data")
            continue

    return jobs


if __name__ == "__main__":
    try:
        get_company_list()
        total_pages = 193
        all_jobs = []

        for page in range(1, total_pages + 1):
            try:
                html_content = fetch_totaljobs_page(page)
                job_listings = extract_job_data(html_content, page)

                if job_listings:
                    all_jobs.extend(job_listings)
                    print(f"✅ Page {page}: {len(job_listings)} jobs matched and added.")
                else:
                    print(f"⚠️ Page {page}: No matching jobs.")
                
                        # Save all matched jobs into a single file
                with open("totaljobs_combined.json", "w", encoding="utf-8") as f:
                    json.dump(all_jobs, f, ensure_ascii=False, indent=4)
                    
                time.sleep(2)  # Be nice to the server

            except Exception as page_error:
                notify_failure(str(page_error), f"Page {page}")
                continue


        notify_success(f"✅ TotalJobs scraping completed.\nTotal jobs matched: {len(all_jobs)}")

    except Exception as final_error:
        notify_failure(str(final_error), "Main Script")

