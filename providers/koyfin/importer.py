#%% Koyfin Importer
from datetime import datetime, timedelta
import sys
sys.path.append('../..')
from sqlite import sqliteDB, getTableName
from providers.nasdaq.get_tickers import getUsExchangeTickers
from providers.koyfin.api import \
  getAllDayCandlesForTicker, getSymbolId, getCandles, getCompletePriceTargetHistory,\
  getTargetPriceHistory, getNumAnalysts

#%% Update day candle info
# For each nasdaq ticker (only US):
#    if there is no data  for ticker:
#      get all available data for that ticker (price and target)
#      update db with this new data
#    if there is already data for the ticker:
#      get last day of data from db
#      get price data from api from last db day until the day before current date
#      update db with this new data
#      
#      get last price target data date in db
#      get price target data from api from last db day until the day before the current date
def updateDayCandleData():
  def mergeCandlesAndPriceTarget(candles, priceTargetHist):
    targets = {}
    last_know_pt = None
    if not priceTargetHist:
      return candles
    for target in priceTargetHist:
      targets[target['date']] = target['value']
    for candle in candles:
      if candle['date'] in targets:
        candle['price_target'] = targets[candle['date']]
        last_know_pt = targets[candle['date']]
      else:
        candle['price_target'] = last_know_pt
    return candles

  tickers = getUsExchangeTickers()
  db = sqliteDB()
  db.connect()
  for ticker in tickers:
    table_name = getTableName('koyfin', ticker['symbol'], '1D')
    koyfinId = getSymbolId(ticker['symbol'])
    num_analysts = getNumAnalysts(koyfinId)
    print('Processing', ticker['symbol'], koyfinId, 'num_analysts:', num_analysts)
    if num_analysts != None and num_analysts > 15:
      if not db.candleTableExists(table_name):
        candles = getAllDayCandlesForTicker(koyfinId)
        priceTargetHist = getCompletePriceTargetHistory(koyfinId)
        new_candles = mergeCandlesAndPriceTarget(candles, priceTargetHist) if priceTargetHist and candles else candles
        if new_candles:
          db.createCandleTableIfNotExists(table_name)
          for candle in new_candles:
            price_target = candle.get('price_target')
            db.insertCandle(table_name, datetime.strptime(candle['date'], '%Y-%m-%d'), candle['open'], candle['high'], candle['low'], candle['close'], candle['volume'], price_target, num_analysts)
      else:
        next_candle_to_get = db.getLastCandleTime(table_name) + timedelta(days=1)
        yesterday = datetime.today() - timedelta(days=1)
        candles = getCandles(koyfinId, datetime.strftime(next_candle_to_get, '%Y-%m-%d'), datetime.strftime(yesterday, '%Y-%m-%d'))
        priceTargetHist = getTargetPriceHistory(koyfinId, next_candle_to_get, yesterday)
        new_candles = mergeCandlesAndPriceTarget(candles, priceTargetHist) if priceTargetHist and candles else candles
        if new_candles:
          for candle in new_candles:
            price_target = candle['price_target'] if 'price_target' in candle else None
            db.insertCandle(table_name, datetime.strptime(candle['date'], '%Y-%m-%d'), candle['open'], candle['high'], candle['low'], candle['close'], candle['volume'], price_target, num_analysts)
  db.disconnect()

""" def addOnlyNumAnalysts():
  db = sqliteDB()
  db.connect()
  tickers = db.getAllNasdaqTickers()
  for ticker in tickers:
    table_name = getTableName('koyfin', ticker, '1D')
    koyfinId = getSymbolId(ticker['symbol'])
    num_analysts = getNumAnalysts(koyfinId)
    lastTime = db.getLastCandleTime(table_name) """


      
#%% Run back test on this data
updateDayCandleData()

# %%
