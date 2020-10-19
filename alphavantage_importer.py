#%%
import datetime
import pandas as pd
from time import sleep
from requests import request
import os
from db import addPrices, addTicker, addSp500Tickers, getAllTickerCodes

base_url = 'https://www.alphavantage.co/query' # https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv
api_key = '0CZNJ0T2BCTOB1UL'

#%%
# Returns dict with date (in YYYY-MM-DD format) as keys
# ['Time Series (Daily)'] ['Meta Data']
def getTimeSeriesDaily(ticker):
  params = {'function': 'TIME_SERIES_DAILY', 'symbol': ticker, 'datatype': 'json', 'outputsize': 'full', 'apikey': api_key}
  resp = request('GET', base_url, params=params).json()
  # resp has two keys: 'Meta Data' and 'Time Series (Daily)'
  prices = resp.get('Time Series (Daily)', [])
  priceList = []
  for date in prices:
    priceList.append((date, prices[date]['1. open'], prices[date]['2. high'], prices[date]['3. low'], prices[date]['4. close'], None, prices[date]['5. volume']))
  return {'ticker': ticker, 'columns': ('price_date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'), 'prices': priceList}

def getCurrentlyListedStocks():
  dateStr = datetime.datetime.now().strftime("%Y-%m-%d")
  params = {'function': 'LISTING_STATUS', 'date': dateStr, 'state': 'active', 'apikey': api_key}
  csvStr = request('GET', base_url, params=params).text
  stockData = csvStr.split('\r\n')
  columns = stockData[0].split(',')
  stocks = [line.split(',') for line in list(filter(lambda row: len(row) > 6, stockData[1:]))]
  return {'columns': columns, 'stockList': list(filter(lambda stock: stock[2] == 'NYSE' or stock[2] == 'NASDAQ' or stock[3] == 'Stock', stocks))}

def loadTickerPrices(ticker):
  tickerInfo = getTimeSeriesDaily(ticker)
  dailyPrices = tickerInfo['prices']
  if len(dailyPrices) != 0:
    rowsAdded = addPrices('alphavantage', ticker, dailyPrices)
    print(ticker, ': Added ', rowsAdded, ' candles to DB')
  else:
    print(ticker, ': No prices to add')
    print(tickerInfo)

def loadTickersIntoDb():
  stocks = getCurrentlyListedStocks()
  numStocksAdded = addTickers(stocks)
  print(numStocksAdded, 'stocks were added')

def loadPricesForAllTickersInDb():
  print('Getting all ticker codes from DB...')
  tickers = getAllTickerCodes()
  #print(tickers)
  print('Loading prices for each ticker...')
  for ticker in tickers:
    loadTickerPrices(ticker)
    sleep(13)

def loadSP500Tickers():
  print('Getting S&P 500 tickers...')
  table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
  df = table[0]
  for index, ticker in df.iterrows():
    rowCount = addTicker(ticker=ticker['Symbol'], name=ticker['Security'], sector=ticker['GICS Sector'], industry=ticker['GICS Sub Industry'])
    added = '' if (rowCount == 1) else 'NOT '
    print('Ticker', ticker['Symbol'], f'{added}added to DB', '; Row count:', rowCount)


#%%
#loadTickersIntoDb()
loadPricesForAllTickersInDb()
#loadTickerPrices('A')
#loadSP500Tickers()
# %%

# db: exch, ticker, name, desc, asset_type, ipo_date, delisting, status, sect, indus
# CSV: symb, name, excch, assetType, ipodate, delisting, status

# %%
