#%% IG API
import requests
import os
import json
from datetime import datetime, timedelta
"""
  What are the default REST Trading API limits?
  Per-app non-trading requests per minute: 60
  Per-account trading requests per minute: 100 (Applies to create/amend position or working order requests)
  Per-account non-trading requests per minute: 30
  Historical price data points per week: 10,000 (Applies to price history endpoints)
"""

class IgApi():
  def login(self):
    self.base_url = 'https://demo-api.ig.com/gateway/deal/'
    self.api_key = os.environ.get('IG_API_KEY')
    user = os.environ.get('IG_USER')
    password = os.environ.get('IG_PASSWORD')
    payload=f'{{"identifier": "{user}", "password": "{password}", "encryptedPassword": null}}'
    headers = {
      'Accept': 'application/json; charset=UTF-8',
      'X-IG-API-KEY': self.api_key,
      'Content-Type': 'application/json; charset=UTF-8'
    }
    response = requests.request("POST", self.base_url + 'session', headers=headers, data=payload)
    self.cst = response.headers.get('CST')
    self.x_security_token = response.headers.get('X-SECURITY-TOKEN')
    resp = json.loads(response.text)
    self.lightstreamer_url = resp['lightstreamerEndpoint']
    self.account_id = resp['currentAccountId']
    self.token_time = datetime.now()
    self.max_token_age = int(response.headers.get('access-control-max-age'))
    return resp
  
  def refreshTokens(self):
    token_age = (datetime.now() - self.token_time).seconds
    one_min = 60
    if token_age >= (self.max_token_age - one_min):
      self.login()
  
  def _getHeaders(self, version):
    return {
      'X-SECURITY-TOKEN': self.x_security_token,
      'CST': self.cst,
      'X-IG-API-KEY': self.api_key,
      'Version': str(version),
      'Content-Type': 'application/json; charset=UTF-8',
      'Accept': 'application/json; charset=UTF-8'
    }

  def getOpenPositions(self):
    """
    Returns and array of positions. Example of a position:
    { 
            "position": {
                "contractSize": 1.0,
                "createdDate": "2021/02/06 17:29:46:000",
                "dealId": "DIAAAAE5WQ7B8AZ",
                "dealSize": 1.0,
                "direction": "BUY",
                "limitLevel": null,
                "openLevel": 40784.0,
                "currency": "USD",
                "controlledRisk": false,
                "stopLevel": null,
                "trailingStep": null,
                "trailingStopDistance": null,
                "limitedRiskPremium": null
            },
            "market": {
                "instrumentName": "Bitcoin ($1)",
                "expiry": "-",
                "epic": "CS.D.BITCOIN.CFD.IP",
                "instrumentType": "CURRENCIES",
                "lotSize": 1.0,
                "high": 41026.0,
                "low": 37810.8,
                "percentageChange": 7.65,
                "netChange": 2895.6,
                "bid": 40707.2,
                "offer": 40802.2,
                "updateTime": "16:31:11",
                "delayTime": 0,
                "streamingPricesAvailable": true,
                "marketStatus": "TRADEABLE",
                "scalingFactor": 1
            }
        }
    """
    response = requests.get(self.base_url + 'positions', headers=self._getHeaders(2)) 
    return response.json()['positions']

  def getPriceHistory(self, epic, resolution, from_dt=None, to_dt=None, max=None):
    """
      resolution: eg. MINUTE_5, DAY
      from_dt: Optional. In format YYYY-MM-DDTHH:MM:SS
      to_dt: Optional. In format YYYY-MM-DDTHH:MM:SS
      max: Optional. number of candles to retrieve
    """
    url = f'{self.base_url}prices/{epic}'
    params = {'resolution': resolution, 'pageSize': '0'}
    if from_dt:
      params['from'] = from_dt.strftime('%Y-%m-%dT%H:%M:%S')
    if to_dt:
      params['to'] = to_dt.strftime('%Y-%m-%dT%H:%M:%S')
    if max:
      params['max'] = max
    response = requests.get(url, headers=self._getHeaders(3), params=params)
    return json.loads(response.text)['prices']
  
  def createOrder(self, order):
    """
    Possible properties of order:
      currencyCode: instrument currency (3 characters)
      dealReference: user defined reference for the deal
      direction: 'BUY' or 'SELL'
      epic: instrument
      forceOpen: default is False
      level: entry level price
      limitDistance: optional
      limitLevel: optional
      size: deal size
      stopDistance: optional
      stopLevel: optional
      timeInForce: 'GOOD_TILL_CANCELLED' or 'GOOD_TILL_DATE'. Default = 'GOOD_TILL_CANCELLED'
      goodTillDate: Pattern(regexp="(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}|\\d*)")] when this type is set in timeInForce
      trailingStop: boolean whether to use trailing stop or not. Default: False
      trailingStopIncrement: increment step in pips for the trailing stop
      type: 'LIMIT' or 'STOP'. LIMIT if current price is higher than entry, STOP otherwise
    """
    order['expiry'] = '-' # For CFDs the expiry is always this value
    order['guaranteedStop'] = 'false'
    if 'forceOpen' not in order:
      order['forceOpen'] = 'false'
    if 'timeInForce' not in order:
      order['timeInForce'] = 'GOOD_TILL_CANCELLED'
    response = requests.post(self.base_url + 'workingorders/otc', headers=self._getHeaders(2), json=order)
    resp = json.loads(response.text)
    if 'dealReference' in resp:
      return resp['dealReference']
    print('Create order response:', response.text)
    return None

  def openPosition(self, position):
    """
    Possible properties of position:
      currencyCode: instrument currency (3 characters)
      dealReference: user defined reference for the deal
      direction: 'BUY' or 'SELL'
      epic: instrument
      forceOpen: default is False
      level: entry level price
      limitDistance: optional
      limitLevel: optional
      orderType: 'LIMIT' or 'MARKET'
      size: deal size
      stopDistance: optional
      stopLevel: optional
      timeInForce: 'EXECUTE_AND_ELIMINATE' or 'FILL_OR_KILL' (the first allow parcial fills)
        - default = 'FILL_OR_KILL'
      trailingStop: boolean whether to use trailing stop or not
        [Constraint: If trailingStop equals true, then set stopDistance,trailingStopIncrement]
      trailingStopIncrement: increment step in pips for the trailing stop
    """
    position['expiry'] = '-' # For CFDs the expiry is always this value
    position['guaranteedStop'] = False
    if 'forceOpen' not in position:
      position['forceOpen'] = True
    if 'timeInForce' not in position:
      position['timeInForce'] = 'FILL_OR_KILL'
    response = requests.post(self.base_url + 'positions/otc', headers=self._getHeaders(2), json=position)
    return response.json()['dealReference']

  def closePosition(self, position):
    """
    Possible properties of position:
      dealId: Get this from the trade confirmation or get positions
      direction: 'BUY' or 'SELL'
      epic
      level
      orderType: 'LIMIT' or 'MARKET'
      size
      timeInForce: 'EXECUTE_AND_ELIMINATE' or 'FILL_OR_KILL' (the first allows partial fills)
        - default = 'EXECUTE_AND_ELIMINATE'
    """
    if 'timeInForce' not in position:
      position['timeInForce'] = 'FILL_OR_KILL'
    if 'level' not in position:
      position['level'] = None
    position['epic'] = None
    position['expiry'] = None
    position['quoteId'] = None
    headers = self._getHeaders(1)
    headers['_method'] = 'DELETE'
    response = requests.post(self.base_url + 'positions/otc', headers=headers, json=position)
    resp = response.json()
    if 'dealReference' in resp:
      return {'deal_ref': resp['dealReference']}
    return resp

  def closeAllPositions(self, epic):
    open_positions = self.getOpenPositions()
    epic_positions = filter(lambda p: p['market']['epic'] == epic, open_positions)
    for pos in epic_positions:
      position = {
        'dealId': pos['position']['dealId'],
        'direction': 'BUY' if pos['position']['direction'] == 'SELL' else 'SELL',
        'epic': epic,
        'orderType': 'MARKET',
        'size': int(pos['position']['dealSize'])
      }
      self.closePosition(position)

  def confirmDeal(self, deal_ref):
    """ Example of what it returns:
    {"date":"2021-02-08T15:21:16.804","status":"OPEN","reason":"SUCCESS","dealStatus":"ACCEPTED","epic":"CS.D.BITCOIN.CFD.IP","expiry":"-","dealReference":"ETNXTLH6X8544TD","dealId":"DIAAAAE52236QAK","affectedDeals":[{"dealId":"DIAAAAE52236QAK","status":"OPENED"}],"level":43772.6,"size":1.0,"direction":"BUY","stopLevel":null,"limitLevel":null,"stopDistance":null,"limitDistance":null,"guaranteedStop":false,"trailingStop":false,"profit":null,"profitCurrency":null}
    In case of error:
    {"errorCode": "error.confirms.deal-not-found"}
    """
    response = requests.get(f"{self.base_url}confirms/{deal_ref}", headers=self._getHeaders(1))
    resp = response.json()
    if 'status' in resp and resp['status'] == 'OPEN':
      return {'deal_id': resp['dealId']}
    return resp






#%%
"""
from datetime import datetime
ig = IgApi()
ig.login()
#positions = ig.getOpenPositions()
epic = 'CS.D.GBPUSD.MINI.IP'
from_dt = datetime(2021, 2, 4, 22, 0)
to_dt = datetime(2021, 2, 5, 8)
resp = ig.getPriceHistory(epic=epic, from_dt=from_dt, to_dt=to_dt)
print(resp)

from datetime import datetime, timedelta
ig = IgApi()
ig.login()
symbol = 'CS.D.GBPUSD.MINI.IP'
today = datetime.now()
yesterday = today - timedelta(days=1)
year = int(today.strftime('%Y'))
today_month = int(today.strftime('%m'))
yesterday_month = int(yesterday.strftime('%m'))
today_date = int(today.strftime('%d'))
yesterday_date = int(yesterday.strftime('%d'))
from_dt = datetime(year, yesterday_month, yesterday_date, 22)
to_dt = datetime(year, today_month, today_date, 8)
hist_prices = ig.getPriceHistory(epic=symbol, resolution='MINUTE_5', from_dt=from_dt, to_dt=to_dt)
"""
#%%