# general
from datetime import datetime
import sys
import pathlib
from tqdm import tqdm
filepath = str(pathlib.Path(__file__).parent.resolve())

# DB.json, upbit_key.json loading
import json

# Web Crawling Framework
import requests

# upbit api
import jwt
import uuid
import hashlib
from urllib.parse import urlencode

# MySQL connector
from api_mysql import mysql_create_session

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

#현재 각 Coin에 대한 이름 데이터를 가져옵니다.
#Output = name에 대한 dict(name_dict)
def get_name_dict():
  url = "https://api.upbit.com/v1/market/all"
  querystring = {
    "isDetails" : "false"
  }
  coin_list = requests.get(url, params=querystring).json()

  name_dict = {}
  for coin in coin_list:
    if 'KRW' in coin['market']: #기본적으로 KRW 마켓에서 거래되는 코인의 'market'플래그는 KRW-BTC 와 같습니다.
      name_dict[coin['market'].split('-')[1]] = coin['korean_name'] # {'BTC' : '비트코인'}과 같이 저장
  
  return name_dict

#현재 각 Coin에 대한 시세, 시가총액 데이터를 가져옵니다.
#Input = name_dict(code 목록을 불러오기 위함)
#Output = price, volume에 대한 dict(price_dict, volume_dict)
def get_price_volume_dict(name_dict):
  #upbit api는 시세와 시가총액 데이터를 다루기 위해 markets 플래그를 통해 어떤 것의 데이터를 요구하는지를 확인합니다.
  #이때 markets엔 'KRW-BTC,KRW-ETH'와 같은 형태로 데이터를 제공해야 합니다.
  #다만 현재 name_dict는 {'BTC' : '비트코인', 'ETH' : '이더리움'} 형태로 저장되어 있으므로, 새로운 list를 생성합니다.
  name_list = ['KRW-' + code for code in name_dict.keys()]

  url = "https://api.upbit.com/v1/ticker"
  querystring = {
    "markets" : ','.join(name_list)
  }
  coin_list = requests.get(url, params=querystring).json()

  price_dict = {}
  volume_dict = {}
  for coin in coin_list:
    code = coin['market'].split('-')[1]
    price_dict[code] = float(coin['trade_price'])
    volume_dict[code] = int(coin['acc_trade_price_24h'])

  return(price_dict, volume_dict)

#현재 각 Coin에 대한 출금수수료 데이터를 가져옵니다.
#Input = name_dict(code 목록을 불러오기 위함)
#Output = cost에 대한 dict(cost_dict)
def get_cost_dict(name_dict):
  code_list = [code for code in name_dict.keys()]
  apikey = json.load(open(filepath + '/upbit_key.json'))
  access_key = apikey['access_key']
  secret_key = apikey['secret_key']
  url = "https://api.upbit.com/v1/withdraws/chance"

  cost_dict = {}
  for code in code_list:
    query = {
      'currency' : code,
      'net_type': code
    }
    query_string = urlencode(query).encode()

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
      'access_key' : access_key,
      'nonce' : str(uuid.uuid4()),
      'query_hash' : query_hash,
      'query_hash_alg' : 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorize_token = 'Bearer {}'.format(jwt_token)
    headers = {"Authorization" : authorize_token}

    response = requests.get(url, params=query, headers=headers).json()

    if 'currency' in response and 'withdraw' in response['currency']['wallet_support']:
      cost_dict[code] = float(response['currency']['withdraw_fee'])
  
  return cost_dict

#얻은 data를 통합하여 실제로 갱신할 coindata list를 생성합니다.
#Output = 갱신될 예정인 coindata를 담은 list(coindata_list)
def create_coindata_list(name_dict, price_dict, volume_dict, cost_dict):
  #cost_dict는 거래중인 모든 coin 중 출금 수수료가 존재하는 coin만 존재합니다.
  #이로 인해 cost_dict의 key값들은 모두 갱신되어야 할 code들입니다.(모두 updatable code)
  #따라서 해당 code들에 대하여 coindata를 생성합니다.
  code_list = [code for code in cost_dict.keys()]

  coindata_list = []
  for code in code_list:
    coindata = Coindata()

    coindata.code = code
    coindata.name = name_dict[code]
    coindata.price = price_dict[code]
    coindata.volume = volume_dict[code]
    coindata.cost = cost_dict[code]
    coindata.calcfee()

    coindata_list.append(coindata)
  
  return coindata_list

#DB(MySQL)에 업데이트합니다.
def db_update(coindata_list):
  conn, cur = mysql_create_session()

  #우선 기존에 있는 db 중 depreceated한 데이터를 삭제합니다.
  updatable_code_list = [coindata.code for coindata in coindata_list]
  foramt_strings = ','.join(['%s'] * len(updatable_code_list))
  sql = "SELECT code FROM transactionfee WHERE (code NOT IN (%s)) AND (market='upbit');" % foramt_strings
  try:
    cur.execute(sql, tuple(updatable_code_list))
    rows = cur.fetchall()
  finally:
    conn.close()
  if len(rows) > 0:
    for row in rows:
      sql = "DELETE FROM transactionfee WHERE (code=%s) AND (market='upbit');"
      conn, cur = mysql_create_session()
      try:
        cur.execute(sql, row[0])
        conn.commit()
      finally:
        conn.close()

  #기존에 row가 존재했는지에 따라 upsert를 진행합니다.
  for coindata in tqdm(coindata_list, desc="coindata updating..."):
    data = ('upbit-%s' % coindata.code, 'upbit', coindata.code, coindata.name, coindata.price, coindata.volume, coindata.cost, coindata.fee)
    data = data+data
    sql = "INSERT INTO transactionfee(datakey, market, code, name, price, volume, cost, fee) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s) \
            ON DUPLICATE KEY \
            UPDATE datakey=%s, market=%s, code=%s, name=%s, price=%s, volume=%s, cost=%s, fee=%s;"
    conn, cur = mysql_create_session()
    try:
      cur.execute(sql, data)
      conn.commit()
    finally:
      conn.close()

def thread_run():
  print("%s : Start updating Coindata for upbit" % datetime.now())
  name_dict = get_name_dict()
  price_dict, volume_dict = get_price_volume_dict(name_dict)
  cost_dict = get_cost_dict(name_dict)
  coindata_list = create_coindata_list(name_dict, price_dict, volume_dict, cost_dict)
  db_update(coindata_list)
  print("%s : Successfully updated 'upbit' transactionfee data!" % datetime.now())
  print('%s : Restarting in a min...' % datetime.now())

if __name__=="__main__":
  try:
    while True:
      try:
        thread_run()
        time.sleep(180) #180초마다 반복해서 실행
      except Exception as e:
        print('%s : ' % datetime.now() + str(e))
        print('%s : Exception occured, Restarting in 5 sec...' % datetime.now())
        time.sleep(5)
  except KeyboardInterrupt:
    print('%s : KeyboardInterrupt(CTRL+C) occured. Stopping...' % datetime.now())
    sys.exit()