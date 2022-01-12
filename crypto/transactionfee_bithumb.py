#general
from datetime import datetime
import sys
import pathlib
filepath = str(pathlib.Path(__file__).parent.resolve())

# DB.json loading
import json

# Web Crawling Framework
import requests
from requests.adapters import HTTPAdapter
from lxml import html
from urllib3.util.retry import Retry

# MySQL connector
import pymysql

# Sleep Timer
import time

#Coindata : 업데이트할 내용을 담은 객체
class Coindata:
  code= '' # 코드(ex. BTC)
  name = 'string' # 이름(ex. 비트코인)
  price = 0 # 시세(단위 : 원)
  volume = 0 # 시가총액(단위 : 원)
  cost = 0 # 출금수수료(단위 : 각 코인)(ex. 0.001 BTC)
  fee = 0 #출금수수료(단위 : 원)

  def calcfee(self):
    self.fee = self.price * self.cost


#현재 각 Coin에 대한 시세와 시가총액 데이터를 가져옵니다.
#Output = name, price, volume에 대한 dict(name_dict, price_dict, volume_dict)
def get_name_price_volume_dict():
  #bithumb 웹페이지보단 모바일 페이지가 크롤링이 쉬우므로, 모바일 페이지를 이용합니다.
  url = "https://m.bithumb.com/"

  session = requests.Session()
  retries = Retry(total=5, backoff_factor=1)
  session.mount('https://', HTTPAdapter(max_retries=retries))

  page = session.get(url, timeout=3)
  tree = html.fromstring(page.content.decode('utf-8'))

  #이름 데이터 추출, dict 형태(ex. {'BTC' : '비트코인'})로 저장
  #시세 데이터 추출, dict 형태(ex. {'BTC' : 480.513})로 저장
  #시가총액 데이터 추출, dict 형태(ex. {'BTC' : 48000000})로 저장
  #data의 idx가 의미하는 대상이 같으므로, 같은 idx로 처리
  name_dict = {}
  price_dict = {}
  volume_dict = {}
  name_str_list = tree.xpath('//li[@data-market="KRW"]//em[@class="tb_coin_name_text"]/text()')
  price_data_list = tree.xpath('//span[contains(@id,"realAsset") and contains(@id, "_KRW")]')
  volume_data_list = tree.xpath('//div[contains(@id,"assetReal") and contains(@id, "_KRW2KRW")]')
  for idx in range(len(price_data_list)):
    code = price_data_list[idx].get('id').replace('realAsset','').replace('_KRW','')
    name = name_str_list[idx]
    price = float(price_data_list[idx].get('data-close').replace(',',''))
    volume = int(float(volume_data_list[idx].get('data-sorting')))
    name_dict[code] = name
    price_dict[code] = price
    volume_dict[code] = volume
  
  return(name_dict, price_dict, volume_dict)

#현재 각 Coin에 대한 출금수수료 데이터를 가져옵니다.
#Output = cost에 대한 dict(cost_dict)
def get_cost_dict():
  #bithumb 출금수수료 데이터 페이지에 접근합니다
  url = "https://www.bithumb.com/customer_support/info_fee"

  session = requests.Session()
  retries = Retry(total=5, backoff_factor=1)
  session.mount('https://', HTTPAdapter(max_retries=retries))

  page = session.get(url, timeout=3)
  tree = html.fromstring(page.content.decode('utf-8'))

  #출금수수료 데이터 추출, dict 형태(ex. {'BTC' : 0.0001})로 저장
  cost_dict = {}
  code_str_list = tree.xpath('//tr[@data-coin]//div[@class="right out_fee"]/../preceding-sibling::td[@class="money_type tx_c"]/text()')
  cost_str_list = tree.xpath('//tr[@data-coin]//div[@class="right out_fee"]/text()')
  for idx in range(len(code_str_list)):
    code = code_str_list[idx].split('(')[1].split(')')[0]
    cost = float(cost_str_list[idx])
    cost_dict[code] = cost

  return(cost_dict)

#얻은 data를 통합하여 실제로 갱신할 coindata list를 생성합니다.
#Output = 갱신될 예정인 coindata를 담은 list(coindata_list)
def create_coindata_list(name_dict, price_dict, volume_dict, cost_dict):
  #현재 cost_dict와 나머지 dict의 구성요소는 총 갯수(종류)가 다릅니다.
  #사유 : 상장폐지(시세는 알 수 없으나 출금은 가능한 경우) 등
  #따라서, cost_dict의 key값(BTC, ETH등)과 그 외의 dict들의 key값의 교집합만 업데이트 해야 합니다.
  updatable_code_list = list(set(price_dict.keys()) & set(cost_dict.keys()))

  coindata_list = []
  for code in updatable_code_list:
    coindata = Coindata()
    coindata.code = code
    coindata.name = name_dict[code]
    coindata.price = price_dict[code]
    coindata.volume = volume_dict[code]
    coindata.cost = cost_dict[code]
    coindata.calcfee()
    
    coindata_list.append(coindata)

  return(coindata_list)

#DB(MySQL)에 업데이트합니다.
def db_update(coindata_list):
  DB = json.load(open(filepath + '/DB.json'))
  cryptodb = pymysql.connect(host=DB['host'], port=DB['port'],
                        user=DB['user'], passwd=DB['passwd'],
                        db=DB['db'], charset=DB['charset'])
  cursor = cryptodb.cursor(pymysql.cursors.DictCursor)

  #우선 기존에 있는 db 중 depreceated한 데이터를 삭제합니다.
  updatable_code_list = [coindata.code for coindata in coindata_list]
  foramt_strings = ','.join(['%s'] * len(updatable_code_list))
  sql = "SELECT code FROM transactionfee WHERE (code NOT IN (%s)) AND (market='bithumb');" % foramt_strings
  cursor.execute(sql, tuple(updatable_code_list))
  rows = cursor.fetchall()
  if len(rows) > 0:
    for row in rows:
      sql = "DELETE FROM transactionfee WHERE (code=%s) AND (market='bithumb');"
      cursor.execute(sql, row['code'])
      cryptodb.commit()

  #기존에 row가 존재했는지에 따라 upsert를 진행합니다.
  for coindata in coindata_list:
    data = ('bithumb-%s' % coindata.code, 'bithumb', coindata.code, coindata.name, coindata.price, coindata.volume, coindata.cost, coindata.fee)
    data = data+data
    sql = "INSERT INTO transactionfee(datakey, market, code, name, price, volume, cost, fee) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s) \
            ON DUPLICATE KEY \
            UPDATE datakey=%s, market=%s, code=%s, name=%s, price=%s, volume=%s, cost=%s, fee=%s;"
    cursor.execute(sql, data)
    cryptodb.commit()

#실제 작업 스레드입니다.
def thread_run():
  name_dict, price_dict, volume_dict = get_name_price_volume_dict()
  cost_dict = get_cost_dict()
  coindata_list = create_coindata_list(name_dict, price_dict, volume_dict, cost_dict)
  db_update(coindata_list)
  print("%s : Successfully updated 'bithumb' transactionfee data!" % datetime.now())


if __name__=="__main__":
  try:
    while True:
      try:
        thread_run()
        time.sleep(60) #60초마다 반복해서 실행
      except Exception as e:
        print('%s : ' % datetime.now() + str(e))
        print('%s : Exception occured, Restarting in 5 sec...' % datetime.now())
        time.sleep(5)
  except KeyboardInterrupt:
    print('%s : KeyboardInterrupt(CTRL+C) occured. Stopping...' % datetime.now())
    sys.exit()