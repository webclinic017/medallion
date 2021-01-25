#%% Simfin api
import requests
import json
from datetime import datetime, timedelta

class SimfinApi():
  def __init__(self):
    self.simfin_api_keys = [
      'N37JIMgUfeCf6QUIuzlzCZVXbgA9Ivc8', #kev
      '3J2I01sJj9sKkl4et2ywYLFdb9ZXv7rI', #jose
      '6FdV0g93nwnGweNn93EUfd3CHWoa5Ylp', #soco
      'CBYrCb6ZQ7Naozt0CLy7y3Om6kCNXoBU', #catholink
      'JpWeaV6mORIdBinnzmRwxx1OdCuRtBi5', #kevbru
      '5ws6xpYlJ3ZCyCiX0HtIR20yvsil56SM' #rivas
    ]
    self.key_idx = 0
    self.base_url = 'https://simfin.com/api/v2/companies/'
    self.v1_base_url = 'https://simfin.com/api/v1/'
  
  def getBaseParams(self):
    return {'api-key': self.simfin_api_keys[self.key_idx]}

  def getCompanysAvailableStmts(self, simfin_id):
    url = f'{self.v1_base_url}companies/id/{simfin_id}/statements/list'
    try:
      while True:
        resp = requests.get(url, params=self.getBaseParams())
        response = json.loads(resp.text)
        if 'error' not in response:
          break
        elif self.key_idx < len(self.simfin_api_keys) - 1:
          self.key_idx += 1
          print(response['error'], 'Changing key', self.key_idx+1 , '/', len(self.simfin_api_keys), 'keys used')
        else:
          print(response['error'], 'No more keys')
          return response
    except:
      print('Exception raised while requesting companys available statements')
      return None
    return response

  def getListOfAllCompanies(self):
    url = self.base_url + 'list'
    resp = requests.get(url, params=self.getBaseParams())
    try:
      response = json.loads(resp.text)
      if 'error' in response:
        print(response['error'])
        return response['error']
      return response
    except:
      return None

  def getCompanyInfo(self, simfin_id=None, ticker=None):
    url = self.base_url + 'general'
    params = self.getBaseParams()
    if simfin_id:
      params['id'] = simfin_id
    elif ticker:
      params['ticker'] = ticker
    resp = requests.get(url, params)
    try:
      response = json.loads(resp.text)
      if 'error' in response:
        print(response['error'])
        return response['error']
      return response
    except:
      return None

  def getQuarterlyStatement(self, quarter, fyear, simfin_id=None, ticker=None):
    "fyear = financial year eg. 2020; quarter = eg. 'q1', 'q2', 'q3', or 'q4'"
    stmt_types = ['pl', 'bs', 'cf', 'derived']
    url = self.base_url + 'statements'
    merged_stmt = {'columns': [], 'data': []}
    for stmt_type in stmt_types:
      #print('merged_stmt', len(merged_stmt['columns']) if 'columns' in merged_stmt else 0)
      while True:
        params = self.getBaseParams()
        if simfin_id:
          params['id'] = simfin_id
        elif ticker:
          params['ticker'] = ticker
        params['fyear'] = fyear
        params['period'] = quarter
        params['statement'] = stmt_type
        resp = requests.get(url, params)
        response = json.loads(resp.text)
        if 'error' not in response:
          break
        elif self.key_idx < len(self.simfin_api_keys) - 1:
          self.key_idx += 1
          print(response['error'], 'Changing key', self.key_idx+1 , '/', len(self.simfin_api_keys), 'keys used')
        else:
          print(response['error'], 'No more keys')
          return response
      stmt = response[0]
      #print('stmt', len(stmt['columns']) if 'columns' in stmt else 0)
      for col in stmt['columns']:
        if col not in merged_stmt['columns']:
          col_idx = stmt['columns'].index(col)
          #print('col:', col, 'idx:', col_idx)
          merged_stmt['columns'].append(col)
          merged_stmt['data'].append(stmt['data'][0][col_idx])
    merged_stmt['columns'] = list(map(lambda x: x.lower().translate(str.maketrans(' /', '__', '.,&()-')), merged_stmt['columns']))
    def getStmtFieldType(name):
      return {
        'quarter': 'varchar(6) primary key',
        'report_date': 'timestamp',
        'publish_date': 'timestamp',
        'restated_date': 'timestamp',
        'source': 'text',
        'ttm': 'boolean',
        'value_check': 'boolean',
        'gross_profit_margin': 'real',
        'operating_margin': 'real',
        'net_profit_margin': 'real',
        'return_on_equity': 'real',
        'return_on_assets': 'real',
        'free_cash_flow_to_net_income': 'real',
        'current_ratio': 'real',
        'liabilities_to_equity_ratio': 'real',
        'debt_ratio': 'real',
        'earnings_per_share_basic': 'real',
        'earnings_per_share_diluted': 'real',
        'sales_per_share': 'real',
        'equity_per_share': 'real',
        'free_cash_flow_per_share': 'real',
        'dividends_per_share': 'real'
      }.get(name, 'int')

    stmt = {}
    stmt['quarter'] = {'val': f'{fyear}_{quarter}', 'type': getStmtFieldType('quarter')}
    for idx, col_name in enumerate(merged_stmt['columns']):
      stmt[col_name] = {
        'val': merged_stmt['data'][idx],
        'type': getStmtFieldType(col_name)
      }

    
    #col_index = stmt['columns'].index(key)
    #return stmt['data'][col_index]
    del stmt['ticker']
    del stmt['fiscal_period']
    del stmt['fiscal_year']
    return stmt

  def getPriceData(self, simfin_id=None, ticker=None):
    url = self.base_url + 'prices'
    params = self.getBaseParams()
    if simfin_id:
      params['id'] = simfin_id
    elif ticker:
      params['ticker'] = ticker
    resp = requests.get(url, params)
    try:
      response = json.loads(resp.text)
      if 'error' in response:
        print(response['error'])
        return response['error']
      return response[0]
    except:
      return None

  def getSharesOutstanding(self,  fyear, simfin_id=None, ticker=None):
    url = self.base_url + 'shares'
    params = self.getBaseParams()
    params['type'] = 'common'
    params['period'] = 'fy'
    params['fyear'] = fyear
    if simfin_id:
      params['id'] = simfin_id
    elif ticker:
      params['ticker'] = ticker
    resp = requests.get(url, params)
    try:
      response = json.loads(resp.text)
      if 'error' in response:
        print(response['error'])
        return response['error']
      return response[0]
    except:
      return None

#%%
#resp  =getListOfAllCompanies()

#resp = getQuarterlyStatement(fyear=2020, quarter='q1', ticker='AAPL')

#resp = getSharesOutstanding(fyear=2020, ticker='AAPL')

#print(resp)
#simfin = SimfinApi()
#stmt = simfin.getQuarterlyStatement(quarter='q1', fyear=2019, ticker='AAPL')

# %%
