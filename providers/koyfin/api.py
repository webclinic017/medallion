#%% Koyfin api
import requests
import json
from datetime import datetime, timedelta

def getSymbolId(symbol):
  url = "https://api.koyfin.com/api/v3/tickers/search"
  payload=f'{{"searchString": "{symbol}", "limit": 50, "primaryOnly": true}}'
  headers = {'content-type': 'application/json;charset=UTF-8'}
  response = requests.request("POST", url, headers=headers, data=payload)
  json_resp = json.loads(response.text)
  if not 'data' in json_resp:
    return None
  search_results = json_resp["data"]
  for result in search_results:
    if result["ticker"] == symbol:
      return result["KID"]
  return None

def getCandles(koyfinSymbolId, dateFrom, dateTo, interval='day'):
  """
  dateFrom: string - Must be of format YYYY-MM-DD
  dateTo: string - Must be of format YYYY-MM-DD
  interval: string - Default: 'day' to retrieve day candles

  returns a list of candles
  """
  url = "https://api.koyfin.com/api/v3/data/graph"
  payload=f'{{"id": "{koyfinSymbolId}", "key": "p_candle_range", "useAdjustedPrice": false, "priceFormat": "both", "candleAggregationPeriod": "{interval}", "dateFrom": "{dateFrom}", "dateTo": "{dateTo}"}}'
  headers = {'content-type': 'application/json;charset=UTF-8'}
  response = requests.request("POST", url, headers=headers, data=payload)
  data = json.loads(response.text)
  return data["graph"] if 'graph' in data else None

def getAllDayCandlesForTicker(koyfinSymbolId):
  yesterday = datetime.today() - timedelta(days=1)
  candles = getCandles(koyfinSymbolId, '2000-01-01', yesterday)
  return candles

def getTargetPriceHistory(koyfinSymbolId, dateFrom, dateTo):
  """
  dateFrom: string - Must be of format YYYY-MM-DD
  dateTo: string - Must be of format YYYY-MM-DD

  returns a list with two fields: date (format YYYY-MM-DD) and value (price to five decimal places)
  """
  url = "https://api.koyfin.com/api/v3/data/graph"
  payload=f'{{"id": "{koyfinSymbolId}", "key": "fest_estpt", "useAdjustedPrice": false, "priceFormat": "standard", "candleAggregationPeriod": "day", "dateFrom": "{dateFrom}", "dateTo": "{dateTo}"}}'
  headers = {'content-type': 'application/json;charset=UTF-8'}
  response = requests.request("POST", url, headers=headers, data=payload)
  try:
    data = json.loads(response.text)
  except:
    data = {}
  return data["graph"] if 'graph' in data else None

def getCompletePriceTargetHistory(koyfinSymbolId):
  yesterday = datetime.today() - timedelta(days=1)
  return getTargetPriceHistory(koyfinSymbolId, '2000-01-01', datetime.strftime(yesterday, '%Y-%m-%d'))

def getAnalystRatings(koyfinSymbolId):
  url = "https://api.koyfin.com/api/v3/data/keys"
  payload=f'{{"ids":[{{"type":"KID","id":"{koyfinSymbolId}"}}],"keys":[{{"key":"p_l","alias":"p_l"}},{{"key":"fest_estpt","alias\":\"fest_estpt\"}},{{"key":"fest_est_ar_strongbuy","alias":"fest_est_ar_strongbuy"}},{{"key":"fest_est_ar_outperform","alias":"fest_est_ar_outperform"}},{{"key":"fest_est_ar_hold","alias":"fest_est_ar_hold"}},{{"key":"fest_est_ar_underperform","alias":"fest_est_ar_underperform"}},{{"key":"fest_est_ar_sell","alias":"fest_est_ar_sell"}},{{"key":"p_c1y","alias":"p_c1y"}},{{"key":"fest_est_ar_avg_no","alias":"fest_est_ar_avg_no"}},{{"key":"fest_estpt_high","alias":"fest_estpt_high"}},{{"key":"fest_estpt_low","alias":"fest_estpt_low"}},{{"key":"fest_estpt_median","alias":"fest_estpt_median"}},{{"key":"fest_estpt_num","alias":"fest_estpt_num"}},{{"key":"fest_estpt_stddev","alias":"fest_estpt_stddev"}}]}}'
  headers = {'content-type': 'application/json;charset=UTF-8'}
  response = requests.request("POST", url, headers=headers, data=payload)
  try:
    data = json.loads(response.text)
    ratings = next(iter(data['KID'].values()))
  except:
    ratings = None
  return ratings

def getNumAnalysts(koyfinSymbolId):
  ratings = getAnalystRatings(koyfinSymbolId)
  try:
    #num_analysts = int(ratings['fest_est_ar_strongbuy']) + int(ratings['fest_est_ar_outperform']) + int(ratings['fest_est_ar_hold']) + int(ratings['fest_est_ar_underperform']) + int(ratings['fest_est_ar_sell'])
    num_analysts = int(ratings['fest_est_ar_strongbuy']['value']) + int(ratings['fest_est_ar_outperform']['value']) + int(ratings['fest_est_ar_hold']['value']) + int(ratings['fest_est_ar_underperform']['value']) + int(ratings['fest_est_ar_sell']['value'])
  except:
    num_analysts = None
  return num_analysts
  


#%%
# symbolId = getSymbolId('A')
# ratings = getAnalystRatings(symbolId)
# print('symbolId', symbolId)
# num_analysts = getNumAnalysts(symbolId)
# print(ratings)

# %%
