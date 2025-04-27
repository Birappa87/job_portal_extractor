import requests
import pandas as pd
import re
import json
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Job(Base):
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    job_title = Column(String(255), nullable=False)
    company_name = Column(String(255), nullable=False)
    company_logo = Column(Text, nullable=True)
    salary = Column(String(100), nullable=True)
    posted_date = Column(Text, nullable=False)
    experience = Column(String(100), nullable=True)
    location = Column(String(255), nullable=True)
    apply_link = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    data_source = Column(String(180), nullable=False)

# Global variables
TOKEN = '7844666863:AAF0fTu1EqWC1v55oC25TVzSjClSuxkO2X4'
chat_id = None
engine = None
Session = None
company_list = []


def init_db():
    """Initialize SQLAlchemy engine and create tables if they don't exist"""
    global engine, Session
    try:
        DATABASE_URL = "postgresql://postgres.gncxzrslsmbwyhefawer:tRIOI1iU59gyK1nk@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"
        
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Check if table exists, create if not
        inspector = inspect(engine)
        if not inspector.has_table('jobs'):
            print("Creating jobs table...")
            Base.metadata.create_all(engine)
            print("Table created successfully")
        else:
            print("Jobs table already exists")
            
        # Test connection
        with engine.connect() as conn:
            print("Database connection test successful")
            
    except Exception as e:
        error_message = f"Failed to initialize database: {str(e)}"
        print(error_message)
        notify_failure(error_message, "init_db")
        raise

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

def notify_failure(error_message, location="Unknown"):
    """Sends a failure notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"❌ GLASSDOOR SCRAPER FAILURE at {timestamp}\nLocation: {location}\nError: {error_message}"
        send_message(TOKEN, message, chat_id)

def notify_success(message):
    """Sends a success notification to Telegram."""
    global chat_id, TOKEN
    if chat_id is None:
        chat_id = get_chat_id(TOKEN)
    if chat_id:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"✅ GLASSDOOR SCRAPER SUCCESS at {timestamp}\n{message}"
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

def insert_jobs_to_db(jobs_data):
    """Insert scraped jobs into the database."""
    if not engine:
        init_db()
    
    session = Session()
    try:
        count = 0
        for job_data in jobs_data:
            job = Job(
                job_title=job_data.get('job_title', ''),
                company_name=job_data.get('company_name', ''),
                company_logo=job_data.get('company_logo', None),
                salary=str(job_data.get('salary')) if job_data.get('salary') else None,
                posted_date=job_data.get('posted_date', ''),
                experience=job_data.get('experience', None),
                location=job_data.get('location', ''),
                apply_link=job_data.get('apply_link', ''),
                description=job_data.get('description', None),
                data_source=job_data.get('data_source', 'glassdoor')
            )
            session.add(job)
            count += 1
        
        session.commit()
        print(f"Successfully inserted {count} jobs into the database")
        notify_success(f"Successfully inserted {count} jobs into the database")
        return count
    except Exception as e:
        session.rollback()
        error_message = f"Failed to insert jobs: {str(e)}"
        print(error_message)
        notify_failure(error_message, "insert_jobs_to_db")
        raise
    finally:
        session.close()

def delete_jobs_by_source(source="Glassdoor"):
    """Delete jobs by data source."""
    if not engine:
        init_db()
    
    session = Session()
    try:
        count = session.query(Job).filter(Job.data_source == source).delete()
        session.commit()
        print(f"Successfully deleted {count} jobs with source '{source}'")
        notify_success(f"Successfully deleted {count} jobs with source '{source}'")
        return count
    except Exception as e:
        session.rollback()
        error_message = f"Failed to delete jobs: {str(e)}"
        print(error_message)
        notify_failure(error_message, "delete_jobs_by_source")
        raise
    finally:
        session.close()

def extract_external_url(cookies, headers, queryString):
    """Extract external URL from Glassdoor."""
    try:
        json_data = [
            {
                'operationName': 'SerpRedirectorQuery',
                'variables': {
                    'baseUrl': 'www.glassdoor.co.uk',
                    'queryString': f'{queryString}',
                },
                'query': 'mutation SerpRedirectorQuery($applyData: ApplyDataInput, $baseUrl: String!, $queryString: String!) {\n  redirector(\n    redirectorContextInput: {applyData: $applyData, baseUrl: $baseUrl, queryString: $queryString}\n  ) {\n    redirectUrl\n    __typename\n  }\n}\n',
            },
        ]

        response = requests.post('https://www.glassdoor.co.uk/graph', cookies=cookies, headers=headers, json=json_data)

        result = response.json()
        if response.status_code == 200:
            url = result[0]['data']['redirector']['redirectUrl']
        else:
            url = None
        
        return url
    except Exception as e:
        error_message = f"Failed to extract external URL: {str(e)}"
        print(error_message)
        notify_failure(error_message, "extract_external_url")
        return None

def extract_description(cookies, headers, jobSearchTrackingKey, jl, queryString):
    """Extract job description from Glassdoor."""
    try:
        json_data = [
        {
            'operationName': 'uilTrackingMutation',
            'variables': {
                'events': [
                    {
                        'eventType': 'JAVASCRIPT_DETECTION',
                        'jobSearchTrackingKey': jobSearchTrackingKey,
                        'pageType': 'SERP',
                    },
                ],
            },
            'query': 'mutation uilTrackingMutation($events: [EventContextInput]!) {\n  trackEvents(events: $events) {\n    eventType\n    resultStatus\n    message\n    clickId\n    clickGuid\n    __typename\n  }\n}\n',
        },
        {
            'operationName': 'JobDetailQuery',
            'variables': {
                'enableReviewSummary': True,
                'jl': jl,
                'queryString': queryString,
                'pageTypeEnum': 'SERP',
                'countryId': 2,
            },
            'query': 'query JobDetailQuery($jl: Long!, $queryString: String, $enableReviewSummary: Boolean!, $pageTypeEnum: PageTypeEnum, $countryId: Int) {\n  jobview: jobView(\n    listingId: $jl\n    contextHolder: {queryString: $queryString, pageTypeEnum: $pageTypeEnum}\n  ) {\n    ...JobDetailsFragment\n    employerReviewSummary @include(if: $enableReviewSummary) {\n      reviewSummary {\n        highlightSummary {\n          sentiment\n          sentence\n          categoryReviewCount\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment JobDetailsFragment on JobView {\n  employerBenefits {\n    benefitsOverview {\n      benefitsHighlights {\n        benefit {\n          commentCount\n          icon\n          name\n          __typename\n        }\n        highlightPhrase\n        __typename\n      }\n      overallBenefitRating\n      employerBenefitSummary {\n        comment\n        __typename\n      }\n      __typename\n    }\n    benefitReviews {\n      benefitComments {\n        id\n        comment\n        __typename\n      }\n      cityName\n      createDate\n      currentJob\n      rating\n      stateName\n      userEnteredJobTitle\n      __typename\n    }\n    numReviews\n    __typename\n  }\n  employerContent {\n    managedContent {\n      id\n      type\n      title\n      body\n      captions\n      photos\n      videos\n      __typename\n    }\n    __typename\n  }\n  employerAttributes {\n    attributes {\n      attributeName\n      attributeValue\n      __typename\n    }\n    __typename\n  }\n  gaTrackerData {\n    jobViewDisplayTimeMillis\n    requiresTracking\n    pageRequestGuid\n    searchTypeCode\n    trackingUrl\n    __typename\n  }\n  header {\n    jobLink\n    adOrderId\n    ageInDays\n    applicationId\n    appliedDate\n    applyUrl\n    applyButtonDisabled\n    categoryMgocId\n    campaignKeys\n    easyApply\n    employerNameFromSearch\n    employer {\n      activeStatus\n      bestProfile {\n        id\n        __typename\n      }\n      id\n      name\n      shortName\n      size\n      squareLogoUrl\n      __typename\n    }\n    expired\n    goc\n    gocId\n    hideCEOInfo\n    indeedJobAttribute {\n      education\n      skills\n      educationLabel\n      skillsLabel\n      yearsOfExperienceLabel\n      __typename\n    }\n    isIndexableJobViewPage\n    isSponsoredJob\n    isSponsoredEmployer\n    jobTitleText\n    jobType\n    jobTypeKeys\n    jobCountryId\n    jobResultTrackingKey\n    locId\n    locationName\n    locationType\n    normalizedJobTitle\n    payCurrency\n    payPeriod\n    payPeriodAdjustedPay {\n      p10\n      p50\n      p90\n      __typename\n    }\n    profileAttributes {\n      suid\n      label\n      match\n      type\n      __typename\n    }\n    rating\n    remoteWorkTypes\n    salarySource\n    savedJobId\n    seoJobLink\n    serpUrlForJobListing\n    sgocId\n    __typename\n  }\n  job {\n    description\n    discoverDate\n    eolHashCode\n    importConfigId\n    jobTitleId\n    jobTitleText\n    listingId\n    __typename\n  }\n  map {\n    address\n    cityName\n    country\n    employer {\n      id\n      name\n      __typename\n    }\n    lat\n    lng\n    locationName\n    postalCode\n    stateName\n    __typename\n  }\n  overview {\n    ceo(countryId: $countryId) {\n      name\n      photoUrl\n      __typename\n    }\n    id\n    name\n    shortName\n    squareLogoUrl\n    headquarters\n    links {\n      overviewUrl\n      benefitsUrl\n      photosUrl\n      reviewsUrl\n      salariesUrl\n      __typename\n    }\n    primaryIndustry {\n      industryId\n      industryName\n      sectorName\n      sectorId\n      __typename\n    }\n    ratings {\n      overallRating\n      ceoRating\n      ceoRatingsCount\n      recommendToFriendRating\n      compensationAndBenefitsRating\n      cultureAndValuesRating\n      careerOpportunitiesRating\n      seniorManagementRating\n      workLifeBalanceRating\n      __typename\n    }\n    revenue\n    size\n    sizeCategory\n    type\n    website\n    yearFounded\n    __typename\n  }\n  reviews {\n    reviews {\n      advice\n      cons\n      countHelpful\n      employerResponses {\n        response\n        responseDateTime\n        userJobTitle\n        __typename\n      }\n      employmentStatus\n      featured\n      isCurrentJob\n      jobTitle {\n        text\n        __typename\n      }\n      lengthOfEmployment\n      pros\n      ratingBusinessOutlook\n      ratingCareerOpportunities\n      ratingCeo\n      ratingCompensationAndBenefits\n      ratingCultureAndValues\n      ratingOverall\n      ratingRecommendToFriend\n      ratingSeniorLeadership\n      ratingWorkLifeBalance\n      reviewDateTime\n      reviewId\n      summary\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n',
        },
    ]

        response = requests.post('https://www.glassdoor.co.uk/graph', cookies=cookies, headers=headers, json=json_data)
        
        result = response.json()
        description = result[1]['data']['jobview']['job']['description']
        return description
    except Exception as e:
        error_message = f"Failed to extract job description: {str(e)}"
        print(error_message)
        notify_failure(error_message, "extract_description")
        return "No description available"

def scrape_glassdoor_jobs(cookies, headers, url):
    """Main function to scrape Glassdoor jobs."""
    # Static parameters for POST
    base_url = "https://www.glassdoor.co.uk/graph"
    base_variables = {
        'excludeJobListingIds': [],
        'filterParams': [
            {'filterKey': 'maxSalary', 'values': '9000000'},
            {'filterKey': 'minSalary', 'values': '250000'},
        ],
        'keyword': '',
        'locationId': 7287,
        'locationType': 'STATE',
        'numJobsToShow': 30,
        'originalPageUrl': url,
        'parameterUrlInput': 'IL.0,10_IS7287',
        'pageType': 'SERP',
        'queryString': 'maxSalary=9000000&minSalary=250000',
        'seoFriendlyUrlInput': 'england-uk-jobs',
        'seoUrl': True,
        'includeIndeedJobAttributes': False
    }

    # Initial page cursor and number
    initial_cursor = None
    initial_page_number = 1

    # Container for scraped jobs
    scraped_jobs = []

    try:
        # Function to fetch jobs for a page
        def fetch_jobs(page_cursor, page_number):
            json_data = [{
                'operationName': 'JobSearchResultsQuery',
                'variables': {
                    **base_variables,
                    'pageCursor': page_cursor,
                    'pageNumber': page_number,
                },
                'query': 'query JobSearchResultsQuery($excludeJobListingIds: [Long!], $filterParams: [FilterParams], $keyword: String, $locationId: Int, $locationType: LocationTypeEnum, $numJobsToShow: Int!, $originalPageUrl: String, $pageCursor: String, $pageNumber: Int, $pageType: PageTypeEnum, $parameterUrlInput: String, $queryString: String, $seoFriendlyUrlInput: String, $seoUrl: Boolean, $includeIndeedJobAttributes: Boolean) {\n  jobListings(\n    contextHolder: {queryString: $queryString, pageTypeEnum: $pageType, searchParams: {excludeJobListingIds: $excludeJobListingIds, filterParams: $filterParams, keyword: $keyword, locationId: $locationId, locationType: $locationType, numPerPage: $numJobsToShow, pageCursor: $pageCursor, pageNumber: $pageNumber, originalPageUrl: $originalPageUrl, seoFriendlyUrlInput: $seoFriendlyUrlInput, parameterUrlInput: $parameterUrlInput, seoUrl: $seoUrl, searchType: SR, includeIndeedJobAttributes: $includeIndeedJobAttributes}}\n  ) {\n    companyFilterOptions {\n      id\n      shortName\n      __typename\n    }\n    filterOptions\n    indeedCtk\n    jobListings {\n      ...JobListingJobView\n      __typename\n    }\n    jobSearchTrackingKey\n    jobsPageSeoData {\n      pageMetaDescription\n      pageTitle\n      __typename\n    }\n    paginationCursors {\n      cursor\n      pageNumber\n      __typename\n    }\n    indexablePageForSeo\n    searchResultsMetadata {\n      searchCriteria {\n        implicitLocation {\n          id\n          localizedDisplayName\n          type\n          __typename\n        }\n        keyword\n        location {\n          id\n          shortName\n          localizedShortName\n          localizedDisplayName\n          type\n          __typename\n        }\n        __typename\n      }\n      footerVO {\n        countryMenu {\n          childNavigationLinks {\n            id\n            link\n            textKey\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      helpCenterDomain\n      helpCenterLocale\n      jobAlert {\n        jobAlertId\n        __typename\n      }\n      jobSerpFaq {\n        questions {\n          answer\n          question\n          __typename\n        }\n        __typename\n      }\n      jobSerpJobOutlook {\n        occupation\n        paragraph\n        heading\n        __typename\n      }\n      showMachineReadableJobs\n      __typename\n    }\n    serpSeoLinksVO {\n      relatedJobTitlesResults\n      searchedJobTitle\n      searchedKeyword\n      searchedLocationIdAsString\n      searchedLocationSeoName\n      searchedLocationType\n      topCityIdsToNameResults {\n        key\n        value\n        __typename\n      }\n      topEmployerIdsToNameResults {\n        key\n        value\n        __typename\n      }\n      topOccupationResults\n      __typename\n    }\n    totalJobsCount\n    __typename\n  }\n}\n\nfragment JobListingJobView on JobListingSearchResult {\n  jobview {\n    header {\n      indeedJobAttribute {\n        skills\n        extractedJobAttributes {\n          key\n          value\n          __typename\n        }\n        __typename\n      }\n      adOrderId\n      ageInDays\n      easyApply\n      employer {\n        id\n        name\n        shortName\n        __typename\n      }\n      expired\n      occupations {\n        key\n        __typename\n      }\n      employerNameFromSearch\n      goc\n      gocId\n      isSponsoredJob\n      isSponsoredEmployer\n      jobCountryId\n      jobLink\n      jobResultTrackingKey\n      normalizedJobTitle\n      jobTitleText\n      locationName\n      locationType\n      locId\n      payCurrency\n      payPeriod\n      payPeriodAdjustedPay {\n        p10\n        p50\n        p90\n        __typename\n      }\n      rating\n      salarySource\n      savedJobId\n      seoJobLink\n      __typename\n    }\n    job {\n      descriptionFragmentsText\n      importConfigId\n      jobTitleId\n      jobTitleText\n      listingId\n      __typename\n    }\n    jobListingAdminDetails {\n      userEligibleForAdminJobDetails\n      __typename\n    }\n    overview {\n      shortName\n      squareLogoUrl\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n'
            }]
            response = requests.post(base_url, cookies=cookies, headers=headers, json=json_data)
            return response.json()

        # Start navigation
        next_cursor = initial_cursor
        page_number = initial_page_number

        while True:
            data = fetch_jobs(next_cursor, page_number)
            try:
                job_listings = data[0]['data']['jobListings']['jobListings']
                pagination_cursors = data[0]['data']['jobListings']['paginationCursors']
            except KeyError:
                error_message = f"Stopping at page {page_number}: Unexpected response structure"
                print(error_message)
                notify_failure(error_message, "scrape_glassdoor_jobs")
                break
            
            for job_item in job_listings:
                header = job_item['jobview']['header']

                _job_tracking_key = header['jobResultTrackingKey']
                _partner_link = header['jobLink']

                _job_link = header.get('seoJobLink')
                _jl = _job_link.split('?jl=')[-1]

                query_string = _partner_link.split("/partner/jobListing.htm?")[-1]
                description = extract_description(cookies, headers, _job_tracking_key, _jl, query_string)

                _partner_link = _partner_link.replace("GD_JOB_AD", "GD_JOB_VIEW")
                external_link = extract_external_url(cookies, headers, _partner_link)

                url = None
                if not external_link:
                    url = header.get('seoJobLink')
                else:
                    url = external_link

                salary = None
                try:
                    salary = job_item['jobview']['header'].get('payPeriodAdjustedPay', {}).get('p50', None)
                except:
                    salary = ''

                company_name = clean_name(header.get('employer', {}).get('name', ''))

                if company_name not in company_list:
                    continue

                job_entry = {
                    "job_title": header.get('jobTitleText', ''),
                    "company_name": company_name,
                    "company_logo": job_item['jobview'].get('overview', {}).get('squareLogoUrl', ''),
                    "salary": salary,
                    "posted_date": f"{header.get('ageInDays', '')} days ago",
                    "experience": None,
                    "location": header.get('locationName', ''),
                    "apply_link": url,
                    "description": description,
                    "data_source": "glassdoor"
                }

                if job_entry['company_name'] is not None:
                    scraped_jobs.append(job_entry)

            print(f"Scraped page {page_number} with {len(job_listings)} jobs.")

            # Dynamically find next cursor
            next_page_cursor = None
            for p_cursor in pagination_cursors:
                if p_cursor.get('pageNumber') == page_number + 1:
                    next_page_cursor = p_cursor.get('cursor')
                    break

            if not next_page_cursor:
                print("No more pages available.")
                break

            next_cursor = next_page_cursor
            page_number += 1

            time.sleep(1)
        
        print(f"Scraped total {len(scraped_jobs)} jobs successfully.")
        return scraped_jobs
        
    except Exception as e:
        error_message = f"Failed during scraping: {str(e)}"
        print(error_message)
        notify_failure(error_message, "scrape_glassdoor_jobs")
        return []

def main():
    """Main function to run the scraper."""
    try:
        # Initialize database
        init_db()
        
        cookies = {
        'gdId': 'a9a3794f-c3cd-4dc3-ba7a-bd24bb8cd9a2',
        'indeedCtk': '1ipgg2g8jk8ii801',
        'rl_page_init_referrer': 'RudderEncrypt%3AU2FsdGVkX18YwXiSsJngik2uKCMUD3VqwUx1sp%2BvgH4%3D',
        'rl_page_init_referring_domain': 'RudderEncrypt%3AU2FsdGVkX18mTosiyE8JBUueuG8L01kt516kMhkqzq8%3D',
        '_optionalConsent': 'true',
        'ki_r': '',
        'ki_s': '240196%3A0.0.0.0.0',
        '_gcl_au': '1.1.1933671595.1745643872',
        '_fbp': 'fb.2.1745643872476.193155387393605754',
        'trs': 'INVALID:SEO:SEO:2021-11-29+09%3A00%3A16.8:undefined:undefined',
        'uc': '8013A8318C98C5172ACA70CF4222A8AAD282B8714CD4AADAA8AB8B9B95BD6A3D51F4CEEA8A10DE766A74B2DA5561A91E679EDE37C16A7B1F66AF1F6FCDE359C48973A874B4F8E8FD3CFFC10792B9AEBA6A2C27B929C93FBB029090B32FA6A89ACDC1996A94C9A1B36C00CA3CEA6A05EE2A2131D99E0789229F4BA87DAC72A45A4CC89960A391E37037E1A04EEDF5BDCD',
        'JSESSIONID': '0D5453235D5FAA154D9F54C9129B8BF6',
        'GSESSIONID': '0D5453235D5FAA154D9F54C9129B8BF6',
        'cass': '0',
        'asst': '1745734925.0',
        'rsSessionId': '1745734925461',
        'AWSALB': 'Kvy0Tqe/BnCYdK7VR0sMFzrWdQQsjW8KedjEjr3vR4qNwEEVuh8phUmI82zacfzXifmnlV03ldRHMLJEjqcI7hs/KFKbdBjc/3gm7baEKJvHCsX6wbN61oeQM4d6',
        'AWSALBCORS': 'Kvy0Tqe/BnCYdK7VR0sMFzrWdQQsjW8KedjEjr3vR4qNwEEVuh8phUmI82zacfzXifmnlV03ldRHMLJEjqcI7hs/KFKbdBjc/3gm7baEKJvHCsX6wbN61oeQM4d6',
        'rsReferrerData': '%7B%22currentPageRollup%22%3A%22%2Fjob%2Fjobs-srch%22%2C%22previousPageRollup%22%3A%22%2Fjob%2Fjobs-srch%22%2C%22currentPageAbstract%22%3A%22%2FJob%2Fjobs-SRCH_%5BPRM%5D.htm%22%2C%22previousPageAbstract%22%3A%22%2FJob%2Fjobs-SRCH_%5BPRM%5D.htm%22%2C%22currentPageFull%22%3A%22https%3A%2F%2Fwww.glassdoor.co.uk%2FJob%2Fengland-uk-jobs-SRCH_IL.0%2C10_IS7287.htm%3FmaxSalary%3D9000000%26minSalary%3D250000%22%2C%22previousPageFull%22%3A%22https%3A%2F%2Fwww.glassdoor.co.uk%2FJob%2Fengland-uk-jobs-SRCH_IL.0%2C10_IS7287.htm%3FmaxSalary%3D9000000%26minSalary%3D250000%22%7D',
        'gdsid': '1745730962237:1745737340938:E1C674D26BDAE98E7710081EDDBAF095',
        'rl_user_id': 'RudderEncrypt%3AU2FsdGVkX18RqGpWEKY86lqrcbwcS1vUF79m2wIchDU%3D',
        'rl_trait': 'RudderEncrypt%3AU2FsdGVkX19DoqB8uifMVvX%2BTgA5sHbj2Sr7C5TdeQhKzR1XQnMLoT1zYXOWdbrv0hMntVgl1mBoTozfl%2FndLyEUKUBvUUJ2b6WTZh2WsmZRI9pJBWCQ%2BseF4yEjqmPwUVBj0WGHSUXv%2FLNQyfGssP8zrtwE2HQmJ3qJ7jEG9HU%3D',
        'rl_group_id': 'RudderEncrypt%3AU2FsdGVkX1%2FlkL6%2BaUQ8cypeemJg%2BAsFkSJZl7nvxH0%3D',
        'rl_group_trait': 'RudderEncrypt%3AU2FsdGVkX1%2BqnzKmUZGdxEPM9ri43zoSJ3vqA65uhq8%3D',
        'OptanonConsent': 'isGpcEnabled=0&datestamp=Sun+Apr+27+2025+12%3A32%3A21+GMT%2B0530+(India+Standard+Time)&version=202407.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1eafc5e2-9db4-4939-85fe-8f9a22faffeb&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0017%3A1&AwaitingReconsent=false',
        'rl_anonymous_id': 'RudderEncrypt%3AU2FsdGVkX19zwRa4k11tZ7%2BdeCXa0tiSW9Srkjau%2BEan68S5ncSG2q5xYe9kf1WJzdk%2FbkmMnWx3x5NeABZG0Q%3D%3D',
        'ki_t': '1745384200319%3B1745730968420%3B1745737343114%3B3%3B27',
        'rl_session': 'RudderEncrypt%3AU2FsdGVkX1%2FtpypN%2BxiswjXxyD4kqoVKsoiloMLSMpCu0dfLoSCd%2BEMUmNV9BB8o6I9QTb21%2B5zFQ90g4CoQVzFFtchm4EN%2FwBNegV02%2FcsTsd31iddcZ5j0APKqp25mW8wWBbmLO78ms0AXy3V0Ng%3D%3D',
        'at': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3NWFkZDk2Ny0yZGM3LTQwYzMtYTY1My0zODllNzBiMWEzNzUiLCJ1IjoiYmlyYXBwYS4wMDFAZ21haWwuY29tIiwidWlkIjoyMTkwNTMyMTcsInJmc2giOjE3NDU3MzgxMTI1OTUsInJtYl9pYXQiOjE3NDU2NDQwNDIyNjksInJtYl9leHAiOjE3NzcxODAwNDIyNjksImF0dCI6InJtIiwiYXV0aG9yaXRpZXMiOls0NSw0NywyOF19.e3OzGk-RsF1GNjoxTPwjaK-ZduVvzcqP1ObJfpIUPfVw1rDCe_UXzGOgnjkhec1She_jZvuq-Sz304IjiVqpMU4pNyGpFfky9R5GdP_TKliQlzi6Ij_zk2fGZBloogT8f6QkbvBPgQ9pCkCtNHZJw85YoQmyMdV07FArjyp4teYV8JU_x86krnV6iJ3DxkdMDn7gqtEPBxJfAUUAMhq4DHOuGd2J2fwSddlKJxvon5agfzvG1VpPbmDpOo_sadGwipHmahNjxebIBUhMYLq4YZw1XnSyB85_i6azAhhMS1urFwNX_a5HERdaGC0tY2zrYT_-SwXomAnQYf45RhCBVg',
        '__cf_bm': 'NTOtGBu2LIMUvPByq5.qp4fjCwbMVYOTsaAJ2Ny.cqU-1745737514-1.0.1.1-v58GUF58D0TCNTvf9SbcgOFbrB6miX6AdnWxb6KCgXjx_jz6dr2DLU7bBheZKcG1FibUskgov8J.UwPbllsdyrruy.Ub6hYMrCLDjmEOGjI',
        '_cfuvid': 'xRlGiXuNwfJ5DJ552sxmdPHoPVKX1nN2f7iTX2bwFHA-1745737514046-0.0.1.1-604800000',
        'bs': 'gEIevmbRsUVADBX3VJS85w:FohxaHtapDDt3gVLDZpz8ii1RV9lruKfR6zvEdLnYNY2U6ijvKXA5XhiBGO5RPVvq_WQmV55egADEM8hemPAHmIqSFgLVdrFnuf8D0eMYn0:qS3jBMYXz2Ae0eBF4NZBc88nm365XAC0UX2S2_YThs0',
        '_dd_s': 'rum=0&expire=1745738412988',
        'cdArr': '0',
        'cdzNum': '7',
    }
        
        headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7,kn;q=0.6',
            'apollographql-client-name': 'job-search-next',
            'apollographql-client-version': '7.171.5',
            'content-type': 'application/json',
            'gd-csrf-token': 'WGdzRzPkm9YKBD_eEika_w:4J82BhpwApC7vyMUWDdhe-VZOfdeWoDnNvJ40Z8YJDNAusLPiWPt-7K_tbcEXGQJfnzIataDCrJAV6YaAzHHig:GG6KqDxAE1hFSx5Rr06AKrpebRevUHAJacq4xQZvExc',
            'origin': 'https://www.glassdoor.co.uk',
            'priority': 'u=1, i',
            'referer': 'https://www.glassdoor.co.uk/',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-arch': '""',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"135.0.7049.115"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="135.0.7049.115", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-model': '"Nexus 5"',
            'sec-ch-ua-platform': '"Android"',
            'sec-ch-ua-platform-version': '"6.0"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36',
            'x-gd-job-page': 'serp',
        }
        
        # First, delete existing Glassdoor jobs
        delete_jobs_by_source("glassdoor")
        
        total_jobs = []
        # Scrape new jobs
        urls = [
            "https://www.glassdoor.co.uk/Job/england-uk-jobs-SRCH_IL.0,10_IS7287.htm?maxSalary=9000000&minSalary=300000",
             "https://www.glassdoor.co.uk/Job/scotland-uk-jobs-SRCH_IL.0,11_IS7289.htm?maxSalary=9000000&minSalary=300000",
            "https://www.glassdoor.co.uk/Job/wales-uk-jobs-SRCH_IL.0,8_IS7290.htm?maxSalary=9000000&minSalary=300000",
            "https://www.glassdoor.co.uk/Job/northern-ireland-uk-jobs-SRCH_IL.0,19_IS7288.htm?maxSalary=9000000&minSalary=300000"
        ]

        for url in urls:
            scraped_jobs = scrape_glassdoor_jobs(cookies, headers, url)
            total_jobs.extend(scraped_jobs)
        
        if total_jobs:
            insert_jobs_to_db(total_jobs)
            
        notify_success(f"Complete scraping process finished. Scraped and inserted {len(total_jobs)} jobs.")
            
    except Exception as e:
        error_message = f"Failed in main function: {str(e)}"
        print(error_message)
        notify_failure(error_message, "main")

if __name__ == "__main__":
    main()