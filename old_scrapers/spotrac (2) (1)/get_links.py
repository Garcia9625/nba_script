import requests
from bs4 import BeautifulSoup
import lxml.etree as ET

def get_urls():
    headers = {
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
        'cookie': 'usprivacy=1N--; _pubcid=97367c99-f597-44b4-97de-9750dde02046; _pubcid_cst=kSylLAssaw%3D%3D; ci_session=6jhlfkb0bhhps923ca17pioh4ptc2llu; _lr_env_src_ats=false; _lr_retry_request=true; _lr_geo_location_state=C; _lr_geo_location=EG',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.101.76 Safari/537.36'
    }
    url = 'https://www.spotrac.com/nba/teams'
    response = requests.get(url, headers=headers)
    dom = ET.HTML(response.text)
    teams = dom.xpath('//span[@class="col-10 col-lg-10 col-md-10 col-sm-10"]/a/@href')
    return teams
teams = get_urls()

for team in teams:
    team_name = team.split('/')[-2]
    # url = "https://www.spotrac.com/nba/brooklyn-nets/yearly"

    payload = {}
    headers = {
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
        'cookie': 'usprivacy=1N--; _pubcid=97367c99-f597-44b4-97de-9750dde02046; _pubcid_cst=kSylLAssaw%3D%3D; ci_session=6jhlfkb0bhhps923ca17pioh4ptc2llu; _lr_env_src_ats=false; _lr_retry_request=true; _lr_geo_location_state=C; _lr_geo_location=EG',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.101.76 Safari/537.36'
    }

    response = requests.request("GET", team, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'lxml')
    dom = ET.HTML(str(soup))
    containers = dom.xpath('(//table[@class="table dataTable rounded-top"])[1]//a/@href')
    with open('links.txt', 'a') as f:
        for container in containers:
            if 'javascript' in container:
                continue
            f.write(container + '\t' + team_name + '\n')
    print(f'team {teams.index(team) + 1 } done and have {len(containers)} links')
    
