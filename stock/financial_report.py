#general
from datetime import datetime
import sys
from tqdm import tqdm
import pathlib

import webdriver_manager
filepath = str(pathlib.Path(__file__).parent.resolve())

# DB.json, dart_apikey.json loading
import json
dart_apikey_list = list(json.load(open(filepath + '/dart_apikey.json')).values())

# Web Crawling Framework
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
session = requests.Session()
retries = Retry(total=5, backoff_factor=1)
session.mount('https://', HTTPAdapter(max_retries=retries))
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--log-level=3')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')

# Binary Zip file handling
from io import BytesIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET

# MySQL connector
import pymysql
DB = json.load(open(filepath + '/DB.json'))
stockdb = pymysql.connect(host=DB['host'], port=DB['port'],
                      user=DB['user'], passwd=DB['passwd'],
                      db=DB['db'], charset=DB['charset'])
cursor = stockdb.cursor(pymysql.cursors.DictCursor)

# Sleep Timer
import time

# LinearRegression Library
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_percentage_error

# opendart api 서비스를 이용하여 db를 업데이트합니다.
# opendart api는 아래와 같은 특성을 갖고 있습니다.
# 1. 각 기업의 고유번호(corp_code)를 통해 데이터를 조회합니다.
# 2. DART OPENAPI LIMIT(개인 유저의 경우, 일당 10000/분당 1000회 CALL 제한)

# 따라서, 우선 기업정보(corp_data포함)를 DB에 저장하고 각 corp_code로 관련된 정보를 불러올 것입니다.

#1. 기업 정보 업데이트
def update_corpdata():
  #참조 : https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002

  #우선 각 기업의 고유번호(corp_code) 전체 목록을 갱신합니다.
  url = "https://opendart.fss.or.kr/api/corpCode.xml"
  params={
    "crtfc_key": dart_apikey_list[0]
  }
  resp = session.get(url,params=params)
  # tree = ET.parse(BytesIO(resp.content.decode('utf-8')))
  # statuscode = tree.find('status').text
  # #DART OPENAPI LIMIT에 도달하여 사용한도를 초과한 경우, dart_apikey_list의 다음 key를 사용합니다.
  # while statuscode == "020":
  #   dart_apikey_list.pop(0)
  #   params['crtfc_key'] = dart_apikey_list[0]
  #   resp = session.get(url,params=params)
  #   try:
  #     #정상적인 응답시, zip file(binary)를 보내오기 때문에 ParseError가 발생합니다.
  #     tree = ET.parse(BytesIO(resp.content))
  #     statuscode = tree.find('status').text
  #   except ET.ParseError:
  #     break

  #제공하는 응답 결과가 binary zip file 형식이므로, 파이썬이 이해할 수 있는 데이터 구조로 변환해주어야 합니다.
  #참조 : https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018
  #참조 : https://ayoteralab.tistory.com/entry/Get-%EB%B0%A9%EC%8B%9D%EC%9D%98-%EC%9B%B9-%EC%84%9C%EB%B9%84%EC%8A%A4Rest-API-%ED%98%B8%EC%B6%9C%ED%95%98%EA%B8%B0-Zip-FILE-binary%ED%8E%B8
  with ZipFile(BytesIO(resp.content)) as zf:
    file_name = zf.namelist()[0]
    zf.extractall(filepath + '/crtfcdata')

  #저장한 파일을 불러와, corp_code의 목록을 생성합니다.
  xmlTree = ET.parse(filepath + '/crtfcdata/' + file_name)
  root = xmlTree.getroot()
  corp_list = root.findall('list')
  
  corp_code_list = [corp.findtext('corp_code') for corp in corp_list if corp.findtext('stock_code') != ' '] #비상장 기업은 제외합니다.

  #이제 각 기업의 corp_code를 이용해 부차적인 정보를 불러옵니다.
  #또한 코드 간소화를 위해 바로 db에 갱신합니다.
  for corp_code in tqdm(corp_code_list, desc="corpdata Updataing..."):
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {
      'crtfc_key' : dart_apikey_list[0],
      'corp_code' : corp_code,
    }
    resp = session.get(url, params=params, timeout=5).json()
    #DART OPENAPI LIMIT에 도달하여 사용한도를 초과한 경우, dart_apikey_list의 다음 key를 사용합니다.
    while resp['status'] == "020":
      dart_apikey_list.pop(0)
      params['crtfc_key'] = dart_apikey_list[0]
      resp = session.get(url, params=params, timeout=5).json()

    time.sleep(1) # DART OPENAPI LIMIT

    corp_code = corp_code #고유번호
    corp_name = resp['corp_name'] #정식명칭
    corp_name_eng = resp['corp_name_eng'] #영문명칭
    stock_name = resp['stock_name'] #종목명(상장사) 또는 약식명칭(기타법인)
    stock_code = resp['stock_code'] #상장회사인 경우 주식의 종목코드
    corp_cls = resp['corp_cls'] #법인구분
    if corp_cls == 'Y':
      stock_market = "KOSPI"
    elif corp_cls == "K":
      stock_market = "KOSDAQ"
    elif corp_cls == "N":
      stock_market = "KONEX"
    elif corp_cls == "E":
      stock_market = "ETC"
    ceo_nm = resp['ceo_nm'] #대표자명
    jurir_no = resp['jurir_no'] #법인등록번호
    bizr_no = resp['bizr_no'] #사업자등록번호
    adres = resp['adres'] #주소
    hm_url = resp['hm_url'] #홈페이지
    ir_url = resp['ir_url'] #IR홈페이지
    phn_no = resp['phn_no'] #전화번호
    fax_no = resp['fax_no'] #팩스번호
    induty_code = resp['induty_code'] #업종코드
    est_dt = resp['est_dt'] #설립일(YYYYMMDD)
    acc_mt = resp['acc_mt'] #결산월(MM)

    data =\
    (corp_code, corp_name, corp_name_eng, stock_name, stock_code, stock_market, ceo_nm,\
      jurir_no, bizr_no, adres, hm_url, ir_url, phn_no, fax_no, induty_code, est_dt, acc_mt)
    data = data + data
    sql =\
    "INSERT INTO corpdata(corp_code, corp_name, corp_name_eng, stock_name,\
      stock_code, stock_market, ceo_nm, jurir_no, bizr_no, adres, hm_url,\
      ir_url, phn_no, fax_no, induty_code, est_dt, acc_mt)\
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s\
      , %s, %s, %s, %s, %s, %s, %s) \
    ON DUPLICATE KEY \
    UPDATE corp_code=%s, corp_name=%s, corp_name_eng=%s, stock_name=%s, \
      stock_code=%s, stock_market=%s, ceo_nm=%s, jurir_no=%s, bizr_no=%s, \
      adres=%s, hm_url=%s, ir_url=%s, phn_no=%s, fax_no=%s, induty_code=%s, \
      est_dt=%s, acc_mt=%s;"
    cursor.execute(sql,data)
    stockdb.commit()
    
#2. KISC(한국표준산업분류) 정보 업데이트
def update_KISC():
  #참조 : https://kssc.kostat.go.kr:8443/ksscNew_web/index.jsp#

  #corpdata DB에서 induty_code list를 수집합니다.
  sql = "SELECT induty_code FROM corpdata"
  cursor.execute(sql)
  rows = cursor.fetchall()
  induty_code_list = [row['induty_code'] for row in rows]
  induty_code_list = list(set(induty_code_list)) #중복 제거

  service = Service(ChromeDriverManager().install())
  driver = webdriver.Chrome(service=service, options=chrome_options)
  driver.implicitly_wait(10)
  for induty_code in tqdm(induty_code_list, desc="KISC Updating..."):
    url = "http://kssc.kostat.go.kr/ksscNew_web/kssc/common/ClassificationContent.do?gubun=1&strCategoryNameCode=001"
    driver.get(url)
    input = driver.find_element(By.XPATH, '//input[@id="strCategoryCodeName"]')
    input.send_keys(induty_code)
    driver.find_element(By.XPATH, '//span[@class="btn_pack medium"]/button').click()
    driver.find_element(By.XPATH, '//tr[@style="cursor:pointer;"]').click()
    induty_name_list = driver.find_element(By.XPATH, '//th[text()="분류명"]/following-sibling::td').text.split('\n')
    induty_name = induty_name_list[0]
    induty_name_eng = induty_name_list[1]
    induty_desc = driver.find_element(By.XPATH, '//th[text()="설명"]/following-sibling::td').text

    data = (induty_code, induty_name, induty_name_eng, induty_desc)
    data = data + data
    sql = "INSERT INTO KISC(induty_code, induty_name, induty_name_eng, induty_desc) \
            VALUES(%s, %s, %s, %s) \
            ON DUPLICATE KEY \
            UPDATE induty_code=%s, induty_name=%s, induty_name_eng=%s, induty_desc=%s;"
    cursor.execute(sql, data)
    stockdb.commit()

#3. 재무제표(FinancialState) 정보 업데이트
def update_FinancialState(stock_market):
  #참조 : https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020

  #수집해야할 기업들의 corp_code를 list로 불러옵니다.
  data = (stock_market) # opendart엔 KOSPI/KOSDAQ 소속 기업들의 정보만 등록되어 있습니다.
  sql = "SELECT corp_code FROM corpdata WHERE stock_market IN (%s)"
  cursor.execute(sql, data)

  rows = cursor.fetchall()
  corp_code_list = [row["corp_code"] for row in rows] # str list의 형태입니다.

  # opendart는 2015년부터의 정보를 제공합니다.
  # 2015년부터의 코드 실행 순간의 전년도까지의 사업계획서를 가져오려 합니다.
  current_year = datetime.now().year
  bsns_year_list = list(range(2015,current_year)) 
  bsns_year_list = [str(bsns_year) for bsns_year in bsns_year_list] # str list로 변환해줍니다.

  # corp_code_list = ['00134477'] For test
  for corp_code in tqdm(corp_code_list, desc="FinancialState Updating..."):
    for bsns_year in bsns_year_list:
      url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
      params = {
        'crtfc_key': dart_apikey_list[0],
        'corp_code': corp_code,
        'bsns_year': bsns_year,
        'reprt_code': '11011', #사업보고서
        'fs_div': 'CFS' #연결재무제표
      }
      resp = session.get(url,params=params).json()
      #DART OPENAPI LIMIT에 도달하여 사용한도를 초과한 경우, dart_apikey_list의 다음 key를 사용합니다.
      while resp['status'] == '020':
        dart_apikey_list.pop(0)
        params['crtfc_key'] = dart_apikey_list[0]
        resp = session.get(url,params=params).json()
      time.sleep(1) # DART OPENAPI LIMIT
      
      if resp["message"] != "정상": #오류가 발생한 경우 skip합니다.
        continue

      rcept_list = resp["list"]
      #rcept_no
      rcept_no = rcept_list[0]['rcept_no']
      #bsns_year
      bsns_year = rcept_list[0]['bsns_year']

      #기본적으론 값에 None을 정의하고, 데이터가 있는 경우 갱신합니다.
      current_assets = None
      total_assets = None
      current_liabilities = None
      total_liabilities = None
      capital = None
      total_equity = None
      total_equity_noncontrol = None
      sales = None
      operating_profit = None
      net_profit = None
      net_profit_noncontrol = None

      #데이터가 '-'와 같은 경우가 있습니다. 이 경우 값이 없는 경우이므로, 0으로 설정해야 합니다.
      #아래 함수가 ValueError시 0으로 바꿔 반환합니다.
      def set_int(data):
        try:
          result = int(data)
        except ValueError:
          result = 0
        return result

      #데이터가 '자 본 금.'과 같이 '.'가 포함되거나 띄어쓰기가 포함되는 경우가 있습니다.
      #미리 텍스트를 정규화시킵니다.
      for rcept in rcept_list:
        for key in rcept.keys():
          rcept[key] = rcept[key].replace('.','').replace(' ','')
          

      for rcept in rcept_list:
        #유동자산
        if current_assets == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "유동자산" or rcept['account_nm'] ==  "유동자산합계" or rcept['account_id'] == "ifrs_CurrentAssets" or rcept['account_id'] == "ifrs-full_CurrentAssets"):
            current_assets = set_int(rcept['thstrm_amount'])

        #자산총계
        if total_assets == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "자산총계" or rcept['account_id'] == "ifrs_Assets" or rcept['account_id'] == "ifrs-full_Assets"):
            total_assets = set_int(rcept['thstrm_amount'])

        #유동부채
        if current_liabilities == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "유동부채" or rcept['account_id'] == "ifrs_CurrentLiabilities" or rcept['account_id'] == "ifrs-full_CurrentLiabilities"):
            current_liabilities = set_int(rcept['thstrm_amount'])

        #부채총계
        if total_liabilities == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "부채총계" or rcept['account_id'] == "ifrs_Liabilities" or rcept['account_id'] == "ifrs-full_Liabilities"):
            total_liabilities = set_int(rcept['thstrm_amount'])

        #(납입)자본금
        if capital == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "자본금" or rcept['account_nm'] == "납입자본" or rcept['account_id'] == "dart_ContributedEquity"):
            capital = set_int(rcept['thstrm_amount'])

        #자본총계(총계)
        if total_equity == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "자본총계" or rcept['account_id'] == "ifrs_Equity"):
            total_equity = set_int(rcept['thstrm_amount'])
        
        #자본총계(비지배)
        if total_equity_noncontrol == None:
          if rcept['sj_nm'] == "재무상태표" and (rcept['account_nm'] == "비지배지분" or rcept['account_id'] == "ifrs_NoncontrollingInterests"):
            total_equity_noncontrol = set_int(rcept['thstrm_amount'])
        
        #매출액
        if sales == None:
          if (rcept['sj_nm'] == "손익계산서" or rcept['sj_nm'] == "포괄손익계산서") and (rcept['account_nm'] == "영업수익" or rcept['account_nm'] == "매출액" or rcept['account_id'] == "ifrs_Revenue" or rcept['account_id'] == "ifrs-full_Revenue"):
            sales = set_int(rcept['thstrm_amount'])

        #영업이익
        if operating_profit == None:
          if (rcept['sj_nm'] == "손익계산서" or rcept['sj_nm'] == "포괄손익계산서") and (rcept['account_nm'] == "영업이익" or rcept['account_id'] == "dart_OperatingIncomeLoss"):
            operating_profit = set_int(rcept['thstrm_amount'])

        #당기순이익(총계)
        if net_profit == None:
          if (rcept['sj_nm'] == "손익계산서" or rcept['sj_nm'] == "포괄손익계산서" or rcept['sj_nm'] == "자본변동표") and (rcept['account_nm'] == "당기순이익" or rcept['account_nm'] == "당기순이익(손실)" or rcept['account_nm'] == "연결당기순이익" or rcept['account_nm'] == "연결당기순이익(손실)") and '|' not in rcept['account_detail']:
            net_profit = set_int(rcept['thstrm_amount'])
        
        #당기순이익(비지배)
        if net_profit_noncontrol == None:
          if (rcept['sj_nm'] == "손익계산서" or rcept['sj_nm'] == "포괄손익계산서" or rcept['sj_nm'] == "자본변동표") and (rcept['account_nm'] == "당기순이익" or rcept['account_nm'] == "당기순이익(손실)" or rcept['account_nm'] == "연결당기순이익" or rcept['account_nm'] == "연결당기순이익(손실)") and '|비지배지분' in rcept['account_detail']:
            net_profit_noncontrol = set_int(rcept['thstrm_amount'])

      #자본총계(지배)
      if total_equity_noncontrol == None: # 비지배지분이 아직 정의되지 않은 경우, 0인 경우입니다.
        total_equity_noncontrol = 0
      try:
        total_equity_control = total_equity - total_equity_noncontrol
      except:
        total_equity_control = None

      #당기순이익(지배)
      if net_profit_noncontrol == None: # 비지배지분이 아직 정의되지 않은 경우, 0인 경우입니다.
        net_profit_noncontrol = 0
      try:
        net_profit_control = net_profit - net_profit_noncontrol
      except:
        net_profit_control = None

      #부채비율
      try:
        debt_ratio = total_liabilities/total_equity * 100
      except:
        debt_ratio = None

      #유동비율
      try:
        current_ratio = current_assets/current_liabilities * 100
      except:
        current_ratio = None

      #ROE
      try:
        roe = net_profit_control/total_equity_control * 100
      except:
        roe = None

      #영업이익률
      try:
        operating_profit_margin = operating_profit/sales * 100
      except:
        operating_profit_margin = None

      #순이익률
      try:
        net_profit_margin = net_profit/sales * 100
      except:
        net_profit_margin = None

      #stock_code
      # data = (corp_code)
      # sql = "SELECT stock_code FROM corpdata WHERE corp_code=%s"
      # cursor.execute(sql, data)
      # stock_code = cursor.fetchall()[0]["stock_code"]

      data =\
        (rcept_no, bsns_year, corp_code, current_assets, total_assets, current_liabilities, total_liabilities, \
          capital, total_equity, total_equity_noncontrol, total_equity_control, sales, operating_profit, \
          net_profit, net_profit_noncontrol, net_profit_control, debt_ratio, current_ratio, roe, operating_profit_margin, net_profit_margin)
      data = data + data
      sql =\
         "INSERT INTO FinancialState(rcept_no, bsns_year, corp_code, current_assets, total_assets, current_liabilities, total_liabilities, \
            capital, total_equity, total_equity_noncontrol, total_equity_control, sales, operating_profit, \
            net_profit, net_profit_noncontrol, net_profit_control, debt_ratio, current_ratio, roe, operating_profit_margin, net_profit_margin) \
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
          ON DUPLICATE KEY \
          UPDATE rcept_no=%s, bsns_year=%s, corp_code=%s, current_assets=%s, total_assets=%s, current_liabilities=%s, total_liabilities=%s, \
            capital=%s, total_equity=%s, total_equity_noncontrol=%s, total_equity_control=%s, sales=%s, operating_profit=%s, \
            net_profit=%s, net_profit_noncontrol=%s, net_profit_control=%s, debt_ratio=%s, current_ratio=%s, roe=%s, operating_profit_margin=%s, net_profit_margin=%s;"
      cursor.execute(sql, data)
      stockdb.commit()

#4. 부가분석(FinancialStateEtc) 정보 업데이트
def update_FinancialStateEtc():
  #FinancialState에서 update 해야할 회사의 목록을 불러옵니다.
  sql = "SELECT DISTINCT corp_code FROM stock.FinancialState"
  cursor.execute(sql)
  rows = cursor.fetchall()
  corp_code_list = [row['corp_code'] for row in rows]

  for corp_code in tqdm(corp_code_list, desc="FinancialStateEtc Updating..."):
    #각 corp_code에 대해서 수록된 기록을 불러옵니다
    sql = "SELECT * FROM stock.FinancialState WHERE corp_code=%s"
    params = (corp_code)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    #data가 단일 년도의 것만 존재할 시, 예측이 어려움으로 pass합니다.
    if len(rows) == 1:
      continue

    #각 data를 list로 변환합니다.
    bsns_year_list = [row['bsns_year'] for row in rows]
    current_assets_list = [row['current_assets'] for row in rows]
    total_assets_list = [row['total_assets'] for row in rows]
    current_liabilities_list = [row['current_liabilities'] for row in rows]
    total_liabilities_list = [row['total_liabilities'] for row in rows]
    capital_list = [row['capital'] for row in rows]
    total_equity_list = [row['total_equity'] for row in rows]
    sales_list = [row['sales'] for row in rows]
    operating_profit_list = [row['operating_profit'] for row in rows]
    net_profit_list = [row['net_profit'] for row in rows]
    debt_ratio_list = [row['debt_ratio'] for row in rows]
    current_ratio_list = [row['current_ratio'] for row in rows]
    roe_list = [row['roe'] for row in rows]
    operating_profit_margin_list = [row['operating_profit_margin'] for row in rows]
    net_profit_margin_list = [row['net_profit_margin'] for row in rows]


    #각 data list에 대해 선형 회귀를 구하는 함수를 만듭니다.
    def linear_regression(bsns_year_list, data_list):
      try:
        #들어온 입력이 list인데, array로 처리되어야 하므로 형변환합니다.
        year_arr = np.array(bsns_year_list)
        data_arr = np.array(data_list)
        #X축은 2차원 배열이여야 하므로, reshape 해줍니다.
        year_arr = year_arr.reshape(-1,1)

        #모델을 학습시킵니다.
        lr = LinearRegression()
        lr.fit(year_arr,data_arr)

        #학습된 모델로 각 연도에 대한 예측값들을 list로 반환합니다.
        #이때 연도는 2015년도(데이터 존재일)부터 코드 실행년도 기준 작년도까지입니다.
        year_list = range(2015,datetime.now().year)
        predict_list = [round(float(lr.predict(np.array(year).reshape(-1,1))), 2) for year in year_list]

        #불안전성 점수를 연산합니다.
        #구해진 모델에 대해 분산값을 의미합니다.
        score = round(mean_absolute_percentage_error(data_arr, lr.predict(year_arr)), 3)
      #일부 data list는 데이터 부족으로 인해 연산이 어려울 수 있습니다.
      except Exception as e:
        predict_list = [None for _ in range(2015,datetime.now().year)]
        score = 0

      return (predict_list, score)
    
    #위 함수를 이용하여 연산합니다.
    current_assets_linear_list, current_assets_linear_score = linear_regression(bsns_year_list, current_assets_list)
    total_assets_linear_list, total_assets_linear_score = linear_regression(bsns_year_list, total_assets_list)
    current_liabilities_linear_list, current_liabilities_linear_score = linear_regression(bsns_year_list, current_liabilities_list)
    total_liabilities_linear_list, total_liabilities_linear_score = linear_regression(bsns_year_list, total_liabilities_list)
    capital_linear_list, capital_linear_score = linear_regression(bsns_year_list, capital_list)
    total_equity_linear_list, total_equity_linear_score = linear_regression(bsns_year_list, total_equity_list)
    sales_linear_list, sales_linear_score = linear_regression(bsns_year_list, sales_list)
    operating_profit_linear_list, operating_profit_linear_score = linear_regression(bsns_year_list, operating_profit_list)
    net_profit_linear_list, net_profit_linear_score = linear_regression(bsns_year_list, net_profit_list)
    debt_ratio_linear_list, debt_ratio_linear_score = linear_regression(bsns_year_list, debt_ratio_list)
    current_ratio_linear_list, current_ratio_linear_score = linear_regression(bsns_year_list, current_ratio_list)
    roe_linear_list, roe_linear_score = linear_regression(bsns_year_list, roe_list)
    operating_profit_margin_linear_list, operating_profit_margin_linear_score = linear_regression(bsns_year_list, operating_profit_margin_list)
    net_profit_margin_linear_list, net_profit_margin_linear_score = linear_regression(bsns_year_list, net_profit_margin_list)

    #DB 업데이트
    for idx, bsns_year in enumerate(range(2015,datetime.now().year)):
      #이때, primary key로는 datakey(corp_code-bsns_year (ex.00145186-2015))를 이용합니다.
      datakey = str(corp_code)+"-"+str(bsns_year)
      sql = """INSERT INTO stock.FinancialStateEtc(datakey, corp_code, bsns_year, current_assets_linear, total_assets_linear, 
      current_liabilities_linear, total_liabilities_linear, capital_linear, total_equity_linear, 
      sales_linear, operating_profit_linear, net_profit_linear, debt_ratio_linear, 
      current_ratio_linear, roe_linear, operating_profit_margin_linear, net_profit_margin_linear, 
      current_assets_linear_score, total_assets_linear_score, current_liabilities_linear_score, 
      total_liabilities_linear_score, capital_linear_score, total_equity_linear_score, 
      sales_linear_score, operating_profit_linear_score, net_profit_linear_score, 
      debt_ratio_linear_score, current_ratio_linear_score, roe_linear_score, 
      operating_profit_margin_linear_score, net_profit_margin_linear_score) 
      VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
      ON DUPLICATE KEY 
      UPDATE datakey=%s, corp_code=%s, bsns_year=%s, current_assets_linear=%s, total_assets_linear=%s, 
      current_liabilities_linear=%s, total_liabilities_linear=%s, capital_linear=%s, total_equity_linear=%s, 
      sales_linear=%s, operating_profit_linear=%s, net_profit_linear=%s, debt_ratio_linear=%s, 
      current_ratio_linear=%s, roe_linear=%s, operating_profit_margin_linear=%s, net_profit_margin_linear=%s, 
      current_assets_linear_score=%s, total_assets_linear_score=%s, current_liabilities_linear_score=%s, 
      total_liabilities_linear_score=%s, capital_linear_score=%s, total_equity_linear_score=%s, 
      sales_linear_score=%s, operating_profit_linear_score=%s, net_profit_linear_score=%s, 
      debt_ratio_linear_score=%s, current_ratio_linear_score=%s, roe_linear_score=%s, 
      operating_profit_margin_linear_score=%s, net_profit_margin_linear_score=%s
      """
      params = (datakey, corp_code, bsns_year, current_assets_linear_list[idx], total_assets_linear_list[idx], current_liabilities_linear_list[idx], \
        total_liabilities_linear_list[idx], capital_linear_list[idx], total_equity_linear_list[idx], sales_linear_list[idx], \
        operating_profit_linear_list[idx], net_profit_linear_list[idx], debt_ratio_linear_list[idx], current_ratio_linear_list[idx], \
        roe_linear_list[idx], operating_profit_margin_linear_list[idx], net_profit_margin_linear_list[idx], \
          current_assets_linear_score, total_assets_linear_score, current_liabilities_linear_score, \
          total_liabilities_linear_score, capital_linear_score, total_equity_linear_score, \
          sales_linear_score, operating_profit_linear_score, net_profit_linear_score, \
          debt_ratio_linear_score, current_ratio_linear_score, roe_linear_score, \
          operating_profit_margin_linear_score, net_profit_margin_linear_score)
      params = params + params
      cursor.execute(sql,params)
      stockdb.commit()





if __name__=="__main__":
  try:
    update_corpdata()
    update_KISC()
    for stock_market in ['KOSPI','KOSDAQ']:
      update_FinancialState(stock_market)
    update_FinancialStateEtc()
    time.sleep(60 * 60 * 24)
  except KeyboardInterrupt:
    print('%s : KeyboardInterrupt(CTRL+C) occured. Stopping...' % datetime.now())
    sys.exit()