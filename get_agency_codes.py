import csv

import requests
from bs4 import BeautifulSoup


r = requests.get(
    'https://bfm.sd.gov/vendor/contactinfo.asp',
    headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'  # noqa
    }
)

r.raise_for_status()

soup = BeautifulSoup(r.text, 'html.parser')

rows = soup.find_all('table')[-1].find_all('tr')[1:]

data = [
    ['agency_code', 'agency_name'],
]

for agency in rows:
    (
        code,
        abbrev,
        agency,
        phone
    ) = [' '.join(x.text.split()) for x in agency.find_all('td')]
    data.append([code, agency])

with open('sd-agency-codes.csv', 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerows(data)
