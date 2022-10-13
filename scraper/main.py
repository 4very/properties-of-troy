from bs4 import BeautifulSoup
import requests
from datetime import datetime
import concurrent.futures
import pandas as pd
import logging
from urllib.parse import urlparse
from urllib.parse import parse_qs

# import re

start = datetime.now()
logging.basicConfig(level=logging.WARNING, format=r'%(levelname)-8s : %(filename)-8s : %(message)s')
MAX_THREADS = 185

# get session id and validate
def get_cookies():

  url = r'https://rensselaer.sdgnys.com/index.aspx'

  # get the asp session id and testcookie
  cookies = requests.get(url, allow_redirects=False).cookies.get_dict()

  # validate session with form data by simulating a button press
  data = {
    '__EVENTTARGET': "btnPublicAccess",
    '__EVENTARGUMENT': "",
    '__VIEWSTATE': r'/wEPDwUJNTI2ODQ5MTMwD2QWAgIBD2QWBgIBDxYCHgdWaXNpYmxlZ2QCAg8WAh8AaBYCAgEPDxYCHgRUZXh0ZWRkAgUPD2QWAh4MYXV0b2NvbXBsZXRlBQNvZmZkGAEFHl9fQ29udHJvbHNSZXF1aXJlUG9zdEJhY2tLZXlfXxYBBQtjYnhSZW1lbWJlcilYmKHkwABqiXEH7yk1rLxsEnWz',
    '__VIEWSTATEGENERATOR': '90059987',
    '__EVENTVALIDATION': '/wEWBgL/usqdCQL9xbGRDAKl1bKzCQK1qbSRCwKKsv3hDgKC3IeGDGY89oAQRIeoFNL3hzSUDNEQ5NEa',
    'txtUserName': '',
    'txtPassword': ''
  }
  requests.post(url, data=data, cookies=cookies, allow_redirects=False)
  return cookies['ASP.NET_SessionId']

# wrapper for requests get that includes cookies
def get_w_cookie(url, *rest):
  return requests.get(url, cookies={"ASP.NET_SessionId": SESSION_ID}, *rest, allow_redirects=False)

# get bs obj from list page
def get_list_page(i):
  url = f"https://rensselaer.sdgnys.com/viewlist.aspx?swis=381700&page={i}"
  res = get_w_cookie(url)
  return BeautifulSoup(res.text, 'html.parser')

# parse query objects from url
def parse_queries(url):
  parsed_url = urlparse(url)
  return parse_qs(parsed_url.query)

# parse table and convert it to dataframe
def list_table_to_df(bs):
  table = bs.find('table', id="tblList")
  data = []

  for row in table.find_all('tr'):
    columns = row.find_all('td')
    if columns[0].get('id') == 'cellSwis': continue
    data.append({
      'Tax ID': columns[1].text.rstrip(chr(160)),
      'Owner': columns[2].text,
      'Street Num': columns[3].text,
      'Street Name': columns[4].text,
      'Long Tax ID': parse_queries(columns[1].a['href'])['printkey'][0]
    })

  return pd.DataFrame(data, columns=data[0].keys()).set_index('Long Tax ID')

# used to standardize formatting for csv files, when i add anything
def save_df(df, file):
  df.to_csv(file)

# all in one function just used to test concurrency
def get_list_and_parse(i):
  try: resp = get_list_page(i)
  except:
    # temp solution
    logging.warning('Caught error, retrying')
    return get_list_and_parse(i)
  return list_table_to_df(resp)

# get randomly generated sessionid
SESSION_ID = get_cookies()

if __name__ == '__main__':

  # start multithredded requests
  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    res = executor.map(get_list_and_parse, range(1,185))

  df = pd.concat(res)
  print(df.describe())

  # for index, row in df.iterrows():
  #   res = (re.match(r'(\d+).(\d*)-(\d+)-?(\d*).?(\d*)\/?(\d*)', row['Tax ID']))
  #   vals = []
  #   for val in res.groups():
  #     vals.append(0 if val == '' else int(val))
  #   if index[-4] != '0': print(index[-4], index, row['Tax ID'])
  #   predid = '{0:03d}{1:03d}{2:04d}{3:03d}{4:03d}{5:04d}'.format(*vals)
  #   if predid != index: print(index, predid, row['Tax ID'])

  save_df(df, '../data/main.csv')
  print(datetime.now()-start)
  # program runs in about 35s on a m1 macbook pro on 30 threads
  # loads 185 pages, and gathers around 18.4k lines
