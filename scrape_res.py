from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import re
import datetime
import dateutil

import config

BASE_URL = "https://smokiespermits.nps.gov/index.cfm?BCPermitTypeID=1"
RES_URL = "https://smokiespermits.nps.gov/mapsiteinfo.cfm?ResourceCode={}"

def create_driver():
    chrome_options = Options()
    # chrome_options.headless = True
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(0.5)

    return driver


def start_session(driver):
    driver.get(BASE_URL)

    elem = driver.find_element_by_name('startreslink')
    elem.click()

    select = Select(driver.find_element_by_name('NumberInParty'))
    select.select_by_value('2')

    elem = driver.find_element_by_id('chgnumpartylink')
    elem.click()

    elem = driver.find_element_by_css_selector("body > div:nth-child(23) > "
            "div.ui-dialog-titlebar.ui-corner-all.ui-widget-header.ui-helper-clearfix.ui-draggable-handle > button")
    elem.click()


def parse_site(content):
    soup = BeautifulSoup(content, 'lxml')
    t = soup.findAll('table')

    meta = {
        'name': t[0].find('h3').text,
        'img_url': t[0].find('img')['src'],
    }

    t1_td = t[1].find_all('td')

    elevation_dict = re.search(r':\s(?P<elev_ft>[\d,]+)\sft.\s\((?P<elev_m>[\d,]+)',
                               t[1].find_all('td')[0].text).groupdict()
    meta.update({k: int(v.replace(',', '')) for k, v in elevation_dict.items()})

    def col_parse(j):
        return re.search(r':\s(.+)', t1_td[j].text).group(1)

    meta['group_size'] = int(col_parse(2))
    meta['capacity'] = int(col_parse(3))
    meta['stock_capacity'] = col_parse(4)
    meta['privy'] = t1_td[5].text

    avail_df = pd.DataFrame(columns=['res_date', 'spots'])

    idx = 0

    if len(t) > 5:
        for tab_i in (5, 6):
            table = t[tab_i]
            tds = table.find_all('td')

            for td in tds:
                cls = td['class']

                if 'calendaravailable' in cls:
                    onclick = td.find('a')['onclick']
                    if 'promtNumNights' in onclick:
                        m = re.search(r'promtNumNights\((\d+),\s(\d+),\s(\d+)', onclick)
                        date = datetime.date(*list(map(int, [m.group(3), m.group(1), m.group(2)])))
                    elif 'addItinRow' in onclick:
                        m = re.search(r"addItinRow\('([\d/]+)", onclick)
                        date = dateutil.parser.parse(m.group(1)).date()
                    else:
                        raise Exception(onclick)

                    spots_text = td.find('p').text

                    spots_m = re.search(r'^\d+', spots_text)
                    if spots_m is not None:
                        spots = int(spots_m.group(0))
                    else:
                        print('couldnt parse spots text', spots_text)
                        spots = None
                elif "calendarfull" in cls:
                    m = re.search(r'\son\s(.+)', td['title'])
                    date = dateutil.parser.parse(m.group(1)).date()
                    spots = 0
                else:
                    continue

                avail_df.loc[idx] = [date, spots]
                idx += 1

    return meta, avail_df


def parse_all_sites(driver, current_date):
    s = requests.Session()

    # Set correct user agent
    selenium_user_agent = driver.execute_script("return navigator.userAgent;")
    s.headers.update({"user-agent": selenium_user_agent})

    for cookie in driver.get_cookies():
        s.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    sites_df = pd.DataFrame(columns=['name', 'img_url', 'elev_ft', 'elev_m', 'group_size', 'capacity',
                                     'stock_capacity', 'privy'])
    sites_df.index.name = 'site_id'
    res_dfs = []

    for site_id in range(1, 118):
        print(f'Parsing site {site_id}...')
        res = s.get(RES_URL.format(site_id))

        if res.status_code != 200 or 'Invalid Request' in res.text:
            print(f'No site {site_id}')
        else:
            meta, res_df = parse_site(res.content.decode('utf-8'))
            res_df['site_id'] = site_id
            res_df['scrape_date'] = current_date
            sites_df.loc[site_id] = meta
            res_dfs.append(res_df)

    res = pd.concat(res_dfs, ignore_index=True, sort=False)

    return sites_df, res


def main():
    driver = create_driver()
    try:
        start_session(driver)

        current_date = datetime.date.today()
        sites_df, res = parse_all_sites(driver, current_date)

        with create_engine(config.DB_URL).connect() as conn:
            # sites_df.to_sql('sites', conn, if_exists='append')
            r = conn.execute(f'DELETE FROM res WHERE scrape_date = "{current_date}"')
            print(f'Deleted {r.rowcount} rows for date {current_date}')
            res.to_sql('res', conn, if_exists='append', index=False)
            print(f'Inserted {res.shape[0]} rows to res table')
    finally:
        driver.close()


if __name__ == '__main__':
    main()
