#!/usr/bin/env python
# coding: utf-8


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

URL = 'https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=4&owner=only&count=100&action=getcurrent'
NEXT_URL = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type=4&SIC=&State=&Country=&CIK=&owner=only&accno=&start=%d&count=100'
OWNER_URL = 'https://www.sec.gov/cgi-bin/own-disp?action=getowner&CIK='
OWNER_URL_NEXT_PAGE = '&type=&dateb=&owner=include&start='
PREFIX = 'https://www.sec.gov'


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
    # create a chromedriver to access the page content
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


def get_symbol_form4(link, link_bak):
    # get the trading ticker from form4 link
    form4 = get_soup(PREFIX+link)
    try:
        symbol = form4.find(text='2. Issuer Name ').parent.next_sibling.next_sibling.next_sibling.next_sibling.text
    except Exception as e:
        try:
            form4 = get_soup(PREFIX+link_bak)
            symbol = form4.find(text='2. Issuer Name ').parent.next_sibling.next_sibling.next_sibling.next_sibling.text
        except Exception as e:
            print('Find symbol error: {}.'.format(e))
            symbol = ''
    return symbol.upper()


def get_prices_form4(link, link_bak):
    # get the trading info from form4 link
    form4 = get_soup(PREFIX+link)
    stock_table = form4.find(text='Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned').parent.parent.parent.parent.parent
    tmp = stock_table.tbody
    if tmp == None:
        form4 = get_soup(PREFIX+link_bak)
        stock_table = form4.find(text='Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned').parent.parent.parent.parent.parent
        tmp = stock_table.tbody
        if tmp == None:
            return
    rows = tmp.findAll('tr')
    nrows = len(rows) # number of rows in Table I of Form-4
    line = []
    trans_date = []
    trans_code = []# 'A' or 'S'
    trans_amt = []
    trans_type = []# 'A' or 'D'
    price = []
    owned_amt = []
    filed_date = form4.find(text='Date').parent.parent.previous_sibling.previous_sibling.findAll('td')[-1].text
    line_number = 0
    for row in rows:
        line_number += 1
        cols = row.findAll('td')
        t_code = cols[3].text.split('(')[0].replace('\n','')
        t_type = cols[6].text.split('(')[0].replace('\n','')
        if (t_code in 'ASPasp') and (t_type in 'ADad') and cols[1].text != '':
            line.append(line_number)
            trans_date.append(cols[1].text.split('(')[0])
            trans_code.append(t_code)            
            trans_amt_str = cols[5].text.replace(',', '').split('(')[0]
            if trans_amt_str: 
                t_amt = int(float(trans_amt_str))
            else:
                t_amt = 0
            trans_amt.append(t_amt)
            trans_type.append(cols[6].text.split('(')[0])
            try:
                p = float(row.find('span', attrs={'class': 'FormText'}).next_sibling.text.split('(')[0])
            except Exception as e:
                p = 0
            price.append(p)
            owned_amt_str = cols[8].text.replace(',', '').split('(')[0]
            if owned_amt_str:
                o_amt = int(float(owned_amt_str))
            else:
                o_amt = 0
            owned_amt.append(o_amt)
        else:
            continue
    return {'Line':line, 'TransDate':trans_date, 'TransCode':trans_code, 'TransAmt':trans_amt, 'TransType':trans_type, 'Price':price, 'OwnedAmt':owned_amt, 'FiledDate':filed_date}


def update_cik_issuer(cik, name, symbol):
    # Add new issuer cik info to database
    DevDB.create_index('MA_SEC_CIKList', [('CIK', 1), ('Type', 1)])
    if type(symbol) == list:
        symbol = symbol[0]
    try:
        record = {'CIK': cik, 'CompanyName': name, 'Type': 'Issuer', 
                  'Symbol': symbol, 'UpdateTime': datetime.now(), 
                  '_id': {'CIK': cik, 'Type': 'Issuer'}}
        DevDB.replace_one('MA_SEC_CIKList', {'_id': record['_id']}, record, upsert=True)
        status = True
    except Exception as e:
        print('Insert/Update CIKList error: {}'.format(e))
        status = False
    return status


def get_relationship_dict(soup):
    # Get the relationship between reporter (insider) and issuer
    table = soup.find('td', text='Filings').parent.parent
    result = dict()
    for row in table.findAll('tr')[1:]:
        cols = row.findAll('td')
        issuer_cik = int(cols[1].text)
        name = cols[0].a.text
        relationship = cols[3].text
        result[issuer_cik] = [relationship, name]
    return result


def get_owner_records(reporter_cik, cik_dict):
    # Get the owner records from SEC insider transaction list
    r = requests.get(OWNER_URL+str(reporter_cik), headers={'User-Agent': USER_AGENT})
    soup = BeautifulSoup(r.content, 'html.parser')
    relationship_dict = get_relationship_dict(soup)
    table = soup.find('table', id='transaction-report')
    if table == None:
        return list()
    result = list()
    price_list = list()
    filed_date = ''
    for row in table.findAll('tr', attrs={'valign': 'top'})[1:]:
        cols = row.findAll('td')
        direction = cols[0].text
        if direction not in 'AD':
            continue
        try:
            issuer_cik = int(cols[10].text)
        except Exception as e:
            continue
        security = cols[11].text
        if '4' in cols[4].text:
            form4_link = cols[4].a['href']
        else:
            continue
        
        detail_link = form4_link[::-1].split('/', 1)[1][::-1] + '/xslF345X03'
        form4_detail_link = detail_link + '/doc4.xml'
        form4_edgar_link = detail_link + '/edgardoc.xml'
        print(form4_link)
        print(form4_detail_link)        
        if issuer_cik not in cik_dict:
            symbol = get_symbol_form4(form4_detail_link, form4_edgar_link)
            update_cik_issuer(issuer_cik, relationship_dict[issuer_cik][1], symbol)
        elif cik_dict[issuer_cik] and (cik_dict[issuer_cik] != 'NONE'):
            symbol = cik_dict[issuer_cik]
        else:
            symbol = get_symbol_form4(form4_detail_link, form4_edgar_link)
            update_cik_issuer(issuer_cik, relationship_dict[issuer_cik][1], symbol)
        dir_indir = cols[6].text
        table_line_number = int(cols[9].text)
        if table_line_number >= 1:
            tmp = get_prices_form4(form4_detail_link, form4_edgar_link)
            if tmp is None:
                return result
            n = len(tmp['Price'])
            line_list = tmp['Line']
            trans_type_list = tmp['TransType']
            trans_code_list = tmp['TransCode']
            trans_date_list = tmp['TransDate']
            trans_amt_list = tmp['TransAmt']
            price_list = tmp['Price']
            owned_amt_list = tmp['OwnedAmt']
            filed_date_list = tmp['FiledDate'].split('/')
            filed_date = '%s-%s-%s' % (filed_date_list[2], filed_date_list[0], filed_date_list[1])
            #generate multiple records based on returned 'tmp'
            for i in range(n):
                if trans_type_list[i] == 'A':
                    buy_or_sell = 'P'
                    if trans_code_list[i] == 'A':
                        trans_type = 'A-Award'
                    elif trans_code_list[i] == 'P':
                        trans_type = 'P-Purchase'
                    else:
                        trans_type = 'Voluntary'
                elif trans_type_list[i] == 'D':
                    buy_or_sell = 'S'
                    trans_type = 'S-Sale'
                else:
                    buy_or_sell = '-'
                    trans_type = '-'
                date_str_list = trans_date_list[i].split('/')
                date_str = '%s-%s-%s' % (date_str_list[2], date_str_list[0], date_str_list[1])
                trans_amt = trans_amt_list[i]
                owned_amt = owned_amt_list[i]
                line_number = line_list[i]
                price = price_list[i]
                update_time = datetime.now()
                record = {'Symbol': symbol, 'IssuerCIK': issuer_cik, 'ReporterCIK': reporter_cik,
                         'Buy/Sell': buy_or_sell, 'TransactionDate': date_str, 'TransactionType': trans_type,
                         'Direct/Indirect': dir_indir, 'TransactedAmt': trans_amt, 'OwnedAmt': owned_amt,
                         'SecurityName': security, 'Relationship': relationship_dict[issuer_cik][0],
                         'LineNumber': line_number, 'FormURL': form4_link, 'FiledDate': filed_date,
                         'Price': price, 'UpdateTime': update_time,
                         '_id': {'Symbol': symbol, 'IssuerCIK': issuer_cik, 'ReporterCIK': reporter_cik, 
                                'TransactionDate': date_str, 'LineNumber': line_number, 'FiledDate': filed_date}}
                result.append(record)
        break # use the first entry to gain access to the form4, then take out all records of interest.
    print(result)
    print('No. of records: ', len(result))
    return result


def upload_form4(data):
    # upload insider transaction results to database collection named MA_SEC_Form4
    DevDB.create_index('MA_SEC_Form4', [('Symbol', 1), ('IssuerCIK', 1), ('ReporterCIK', 1), 
                                        ('TransactionDate', 1), ('LineNumber', 1), ('FiledDate', 1)])
    if data:
        now = datetime.now(timezone('US/Eastern'))
        dt_string = now.strftime("%y/%m/%d %H:%M:%S %Z%z")
        try:
            result = DevDB.insert_many('MA_SEC_Form4', data, ordered=False)
            print('Inserted: %d' % (len(result.inserted_ids)))
        except BulkWriteError as e:
            print('Partial inserted: %d' % (len(data)-len(e.details['writeErrors'])))
    return


def update_cik_reporter(cik, name):
    # Add new reporter info to database
    DevDB.create_index('MA_SEC_CIKList', [('CIK', 1), ('Type', 1)])
    try:
        record = {'CIK': cik, 'CompanyName': name, 'Type': 'Reporter', 
                'UpdateTime': datetime.now(), '_id': {'CIK': cik, 'Type': 'Reporter'}}
        DevDB.replace_one('MA_SEC_CIKList', {'_id': record['_id']}, record, upsert=True)
        status = True
    except Exception as e:
        print('Insert/Update CIKList error: {}'.format(e))
        status = False
    return status


cik_dict = load_ciks()
# get SEC most recent Form4 reports from reporters

table_list = list()
soup = get_soup(URL)
for i in range(20):#scrape up to 20 pages each time
    table = soup.findAll('tr', attrs={'nowrap': 'nowrap'})
    if not table:
        # log_file.close()
        break
    table_list.append(table)
    soup = get_soup(NEXT_URL % ((i+1)*100))
for i in range(20):
    table = table_list[i]
    j = 0
    for row in table:# each page has 100 rows
        j += 1
        columns = row.findAll('td')
        if len(columns) < 6:
            j -= 1
            continue
        cik = int(columns[1].a['href'].split('/')[4])
        filed_date = columns[4].text
        filed_dt = datetime.strptime(filed_date, '%Y-%m-%d')
        print(columns[3].text, cik)
        if datetime.now() - filed_dt > timedelta(days=500):
            break
        s = row.previous_sibling.previous_sibling.text
        str_list = s.split('(0')
        name_reporter = str_list[0][:-1].replace('\n', '')
        cik_reporter = int(str_list[1].split(')')[0])
        update_cik_reporter(cik_reporter, name_reporter)
        upload_form4(get_owner_records(cik, cik_dict))
        print('********** '+str(j)+' **********')
    print('Page ' + str(i) + ' completed. ********************\n')

