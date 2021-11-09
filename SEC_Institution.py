#!/usr/bin/env python
# coding: utf-8

import structlog
import requests
from pymongo.errors import DuplicateKeyError, BulkWriteError
from MongoDB.client import DevDB
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import timezone
from latest_user_agents import get_latest_user_agents

import sys

URL = 'https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=13F&owner=include&count=100&action=getcurrent'
PREFIX = 'https://www.sec.gov'
log_file = open('log.sec_13f', 'a', encoding='utf-8')
logger = structlog.PrintLogger(log_file)


def user_agent():
    # get latest user agents
    user_agents = get_latest_user_agents()
    user_agent = ''
    for i in user_agents:
        if sys.platform == 'darwin' and ('Macintosh' in i):
            user_agent = i
        if sys.platform == 'linux' and ('Linux' in i) and ('Android' not in i):
            user_agent = i
    if not user_agent:
        user_agent = user_agents[4]
    return user_agent


USER_AGENT = user_agent()


def get_soup(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--user-agent=%s" % USER_AGENT)
    service = Service('/usr/bin/chromedriver')
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()
    return soup


def load_ciks():
    # load the cik dictionary from MongoDB
    cik_list = list(DevDB.find('MA_SEC_CIKList', {'Type': 'Issuer'}))
    result = dict()
    for cik in cik_list:
        result[cik['CIK']] = cik['Symbol']
    return result


def get_single_13F(link, date):
    # get single 13F filing list
    soup = get_soup(PREFIX + link)
    new_link = ''
    for row in soup.findAll(text='INFORMATION TABLE'):
        item = row.parent.previous_sibling.previous_sibling
        if item.text[-4:] == 'html':
            new_link = item.a['href']
            break
    if not new_link:
        return list()
    data = get_soup(PREFIX + new_link)
    table = data.find('table', attrs={'summary':'Form 13F-NT Header Information'})
    if table == None:
        return list()
    result = list()
    cik = int(new_link.split('/')[4])
    for row in table.tbody.findAll('tr'):
        if not row.findAll('td', attrs={'class': 'FormData'}):
            continue
        cols = row.findAll('td')
        name = cols[0].text
        class_title = cols[1].text
        cusip = cols[2].text
        value = float(cols[3].text.replace(',', '')) * 1000
        amount = int(cols[4].text.replace(',', ''))
        amt_type = cols[5].text
        if cols[6].text != '\xa0':
            amt_type += ' ' + cols[6].text
        investment_discretion = cols[7].text
        other = '' if cols[8].text == '\xa0' else cols[8].text
        voting_sole = int(cols[9].text.replace(',', '')) if cols[9].text == '\xa0' else 0
        voting_shared = int(cols[10].text.replace(',', '')) if cols[10].text == '\xa0' else 0
        voting_none = int(cols[11].text.replace(',', '')) if cols[11].text == '\xa0' else 0
        record = {'CIK': cik, 'FiledDate': date, 'CUSIP': cusip, 'Name': name, 'Class': class_title,
                 'Value': value, 'Amount': amount, 'AMTType': amt_type, 'InvestmentDiscretion': investment_discretion,
                 'Other': other, 'VotingSole': voting_sole, 'VotingShared': voting_shared, 'VotingNone': voting_none,
                 'FormURL': new_link, 'UpdateTime': datetime.now(), 
                  '_id': {'CIK': cik, 'FiledDate': date, 'CUSIP': cusip, 'Class': class_title,
                          'Amount': amount, 'AMTType': amt_type}}
        result.append(record)
    return result


def upload_13F(data):
    # upload institution transaction results to database collection named MA_SEC_13F
    DevDB.create_index('MA_SEC_13F', [('CIK', 1), ('FiledDate', 1), ('CUSIP', 1), 
                                        ('Class', 1), ('Amount', 1), ('AMTType', 1)])
    if data:
        now = datetime.now(timezone('US/Eastern'))
        dt_string = now.strftime("%y/%m/%d %H:%M:%S %Z%z")
        try:
            result = DevDB.insert_many('MA_SEC_13F', data, ordered=False)
            # logger.info('Inserted: %d' % (len(result.inserted_ids)))
            print('Inserted: %d' % (len(result.inserted_ids)))
        except BulkWriteError as e:
            # logger.info('Partial inserted: %d' % (len(data)-len(e.details['writeErrors'])))
            print('Partial inserted: %d' % (len(data)-len(e.details['writeErrors'])))
    return


def update_cik(data):
    # update institution CIK to MA_SEC_CIKList
    str_list = data.previous_sibling.previous_sibling.text.split('(')
    name = str_list[0][:-1].replace('\n', '')
    cik = int(str_list[1].split(')')[0])
    # Add new issuer cik info to database
    DevDB.create_index('MA_SEC_CIKList', [('CIK', 1), ('Type', 1)])
    try:
        record = {'CIK': cik, 'CompanyName': name, 'Type': 'Institution', 
                  'UpdateTime': datetime.now(), '_id': {'CIK': cik, 'Type': 'Institution'}}
        DevDB.replace_one('MA_SEC_CIKList', {'_id': record['_id']}, record, upsert=True)
        status = True
    except Exception as e:
        # logger.warning('Insert/Update CIKList error: {}'.format(e))
        print('Insert/Update CIKList error: {}'.format(e))
        status = False
    return status


def get_history_13F(link, limit=4):
    # get recent limit number of 13F filings of a single institution
    soup = get_soup(PREFIX + link)
    count = 0
    result = list()
    for document in soup.findAll('a', attrs={'id': 'documentsbutton'}):
        if count > limit - 1:
            break
        filed_date = document.parent.next_sibling.next_sibling.next_sibling.next_sibling.text
        result.extend(get_single_13F(document['href'], filed_date))
        count += 1
    return result


cik_list = load_ciks()
soup = get_soup(URL)
table = soup.findAll('tr', attrs={'nowrap': 'nowrap'})
if not table:
    log_file.close()
    table = list()
for row in table:
    columns = row.findAll('td')
    cik = int(columns[1].a['href'].split('/')[4])
    filed_date = columns[4].text
    filed_dt = datetime.strptime(filed_date, '%Y-%m-%d')
    print(filed_date, cik)
    # only scan filings of recent 5 days
    if datetime.now() - filed_dt > timedelta(days=5):
        break
    update_cik(row)
    upload_13F(get_history_13F(columns[-1].a['href'], 8))

log_file.close()
