#%% Nasdaq api
import requests
import json

def getTickers(exchange):
  """
      Returns a list of symbols with the following fields for the specified exchange:
      symbol, name, lastsale, netchange, pctchange, volume, marketCap, country, ipoyear, industry, sector, url

      exchange: string - Can be one of the following: 'NASDAQ', 'NYSE', 'AMEX'
  """
  url = f'https://api.nasdaq.com/api/screener/stocks?exchange={exchange}&country=united_states&download=true'
  payload={}
  headers = {'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
  response = requests.request("GET", url, headers=headers, data=payload)
  ticker_list = json.loads(response.text)['data']['rows']
  filtered_tickers = []
  for ticker in ticker_list:
    if int(ticker['volume']) and ticker['marketCap'] and int(float(ticker['marketCap'])):
      ticker['exchange'] = exchange
      filtered_tickers.append(ticker)
  return filtered_tickers

def getUsExchangeTickers():
  nasdaq = getTickers('NASDAQ')
  print('Got NASDAQ tickers')
  nyse = getTickers('NYSE')
  print('Got NYSE tickers')
  amex = getTickers('AMEX')
  print('Got AMEX tickers')
  return sorted(nasdaq + nyse + amex, key = lambda ticker: ticker['symbol'])

#%% Test
# tickers = getUsExchangeTickers()
# for ticker in tickers:
#   print(ticker['symbol'])
# %%

