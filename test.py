import json
from bs4 import BeautifulSoup
from rnet import Client, Impersonate
import asyncio

async def parse_url(url):
    client = Client(impersonate=Impersonate.Firefox136)
    resp = await client.get(url)
    print("Status Code:", resp.status_code)

    content = await resp.text()
    soup = BeautifulSoup(content, "html.parser")

    # description = extract_job_description(content)  # Assuming you already have this function

    external_url = url  # default fallback

    # Try to find the JSON that includes 'companyApplyUrl'
    try:
        # Search all <code> tags, sometimes LinkedIn puts JSON in <code> or <script> tags
        code_tags = soup.find_all(['code', 'script'])
        for tag in code_tags:
            if tag.string and 'companyApplyUrl' in tag.string:
                data = tag.string
                data = data.replace('&quot;', '"')  # replace HTML encoded quotes
                try:
                    json_data = json.loads(data)
                    external_url = json_data.get('applyMethod', {}).get('companyApplyUrl', url)
                    break
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print("Error while parsing external URL:", str(e))

    print(external_url)

asyncio.run(parse_url('https://www.linkedin.com/jobs/view/4217039106/?alternateChannel=search&refId=BTiLRibMF0oNl5%2BeO%2FkizQ%3D%3D&trackingId=48IT0%2FRH87LTvQytiK%2BhCQ%3D%3D'))