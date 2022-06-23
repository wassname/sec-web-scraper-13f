from unicodedata import name
import pandas as pd
from requests.adapters import HTTPAdapter, Retry
import requests_cache
from pathlib import Path
import re
import csv
import lxml
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
import logging, sys

logger = logging.getLogger(__file__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# cache and retry
session = requests_cache.CachedSession('.demo_cache')
retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ])
session.mount('http://', HTTPAdapter(max_retries=retries))

sec_url = 'https://www.sec.gov'

def get_request(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate, br',
        'HOST': 'www.sec.gov',
    }
    return session.get(url, headers=headers)

def create_url(cik):
    return 'https://www.sec.gov/cgi-bin/browse-edgar?CIK={}&owner=exclude&action=getcompany&type=13F-HR'.format(cik)

def get_user_input():
    cik = eval(input("Enter 10-digit CIK number: "))
    return cik


def scrap_company_report(requested_cik, name):
    # Find mutual fund by CIK number on EDGAR
    url = create_url(requested_cik)
    logger.debug(f"index '{url}'")
    response = get_request(url)
    soup = BeautifulSoup(response.text, "html.parser")
    main = soup.find(id="seriesDiv")
    rows = main.findAll('tr')[1:] # skip header
    if len(rows)==0:
        logger.warn(f"no reports for {name} {url}")
    
    for row in rows[:2]:
        date = row.findAll('td')[3].text
        tag = row.find('a', id="documentsbutton")
        last_report = (sec_url + tag['href'])
        logger.debug(f"scrap_report_by_url '{last_report}' '{name}/{date}.csv'")
        scrap_report_by_url(last_report, f"{name}/{date}")


def scrap_report_by_url(url, name):
    response_two = get_request(url)
    soup_two = BeautifulSoup(response_two.text, "html.parser")
    tags_two = soup_two.findAll('a', attrs={'href': re.compile('xml')})
    xml_url = tags_two[2].get('href')
    response_xml = get_request(sec_url + xml_url)
    soup_xml = BeautifulSoup(response_xml.content, "html.parser")
    table = soup_xml.find(summary="Form 13F-NT Header Information")
    df = pd.read_html(str(table), header=[1, 2])[0]
    df.columns = [(b if a.startswith('Unnamed') else f"{a} {b}")for a,b in df.columns ]
    # df.columns = [" ",] + list(df.columns[1:])

    fo = Path(f"output/{name}.csv")
    fo.parent.mkdir(exist_ok=True)
    df.to_csv(fo, index=False)
    logger.info(fo)

# List of Investments
CIK_LIST = [
    {
    'name': 'Buffett',
    'cik': '0001067983'
}, {
    'name': 'JPMorgan',
    'cik': '0000019617'
}, {
    'name': 'Bridgewater',
    'cik': '0001350694'
}, {
    'name': 'Renaissance',
    'cik': '0001037389'
}, {
    'name': 'TwoSigma',
    'cik': '0001179392'
}, {
    'name': 'DEShaw',
    'cik': '0001009207'
}, {
    'name': 'Millenium',
    'cik': '0001273087'
}, {
    'name': 'Bluecrest',
    'cik': '0001610880'
}, {
    'name': 'AQR',
    'cik': '0001167557'
},
{
    'name': 'Scion Asset Management',
    'cik': '0001649339'
},
{
    'name': 'KYNIKOS ASSOCIATES LP',
    'cik': '0001446440'
},
]
for row in tqdm(CIK_LIST):
    scrap_company_report(row['cik'], row['name'])
