import logging
from datetime import datetime
import pandas as pd
import numpy as np
import sys
sys.path.append('..')
#from providers.ib.IbTrader import IbTrader
from sqlite import sqliteDB, getTableName
from util import chainList, chart

def getData(provider, symbol, candle_size, start_date, end_date):
  db = sqliteDB()
  db.connect()
  table_name = getTableName(provider, symbol, candle_size)
  candleDf = db.getCandles(table_name, start_date, end_date)
  db.disconnect()
  return candleDf

def getBullishEngulfingCol(candleDf):
  # calculate trend
  pct_change = (candleDf['close'] - candleDf['close'].shift(1))/candleDf['close'].shift(1)*100
  avg_change = pct_change.rolling(5).mean()
  conditions = [
      (avg_change > 0),
      (avg_change < 0),
      (avg_change == 0)]
  choices = ['BULL', 'BEAR', 'LAT']
  trend = np.select(conditions, choices)
  bear_trend = trend == 'BEAR'

  # conditions for engulfing
  # 1. Previous candle is red
  prev_is_red = candleDf['open'].shift(1) > candleDf['close'].shift(1)
  # 2. Actual candle is green
  act_is_green = candleDf['open'] < candleDf['close']
  # 3. Actual engulfs Previous
  engulfs = np.logical_and(candleDf['open'] < candleDf['close'].shift(1), candleDf['close'] > candleDf['open'].shift(1))
  #print(pd.DataFrame([prev_is_red, act_is_green, engulfs], axis=1))
  # add engulfing column to indicate engulfing candles
  #return np.where(prev_is_red & act_is_green & engulfs & bear_trend, True, False)
  return np.where(prev_is_red & act_is_green & engulfs, True, False)

def executeStrategy(candleDf, stratOpts):
  tpR = stratOpts['tpR']
  closeOrderAfter = stratOpts['closeOrderAfter']
  retracementLevelPct = stratOpts['retracementLevelPct']
  print('TP R:', tpR)
  orders = []
  positions = []
  trades = []
  for index, candle in candleDf.iterrows():
    logging.debug(f'CANDLE{candle["date"]}')
    if index > 0:
      prevCandle = candleDf.loc[index-1]
      if prevCandle.engulfing == True:
        logging.debug(f'Previous is ENGULFING. pH:{prevCandle.high} pL:{prevCandle.low} retPct:{retracementLevelPct}')
        retracementLevel = ((prevCandle.high - prevCandle.low) * retracementLevelPct / 100) + prevCandle.low
        sl = prevCandle.low * 0.9995
        #tpAnotherEngCandleLength = candleDf.loc[index-1, 'close'] - candleDf.loc[index-1, 'open'] + candleDf.loc[index-1, 'close']
        static_r_tp = ((retracementLevel - sl) * tpR) + retracementLevel
        if candle.low < retracementLevel:
            logging.debug('Open pos as candle low is below retracement level')
            position = {
              'price_bought': retracementLevel,
              'sl': sl,
              'tp': static_r_tp,
              'date_bought': candle.date
            }
            positions.append(position)
        elif candle.low > retracementLevel:
          logging.debug(f'Place order for when price reaches {retracementLevel}')
          order = {
            'buy_at': retracementLevel,
            'sl': sl,
            'candles_open': 0,
            'placed': candle.date
          }
          order['tp'] = static_r_tp
          orders.append(order)
      if bool(len(orders)): # If there are orders placed, check if we can open position or we have to cancel
        for order in orders:
          if candle.low <= order['buy_at']:
            logging.debug(f'Order placed {order["placed"]} filled. Open pos at {order["buy_at"]}')
            #print('buy')
            position = {
              'price_bought': order['buy_at'],
              'sl': order['sl'],
              'tp': order['tp'],
              'date_bought': candle.date
            }
            order['remove'] = True
            positions.append(position)
          else:
            if order['candles_open'] > closeOrderAfter:
              logging.debug(f'Cancelling order placed on {order["placed"]}')
              order['remove'] = True
            else:
              order['candles_open'] += 1
        for order in orders:
          if 'remove' in order:
            orders.remove(order)
      if bool(len(positions)):
        for position in positions:
          if candle.low <= position['sl']:
            #print('sell sl')
            trade = {
              'price_bought': position['price_bought'],
              'price_sold': position['sl'],
              'date_bought': position['date_bought'],
              'date_sold': candle.date
            }
            trade['profit'] = trade['price_sold'] - trade['price_bought']
            trade['profit_mlt'] = trade['profit']  / trade['price_bought'] + 1
            trade['r'] = (position['tp'] - trade['price_bought']) / (trade['price_bought'] - position['sl'])
            trades.append(trade)
            logging.debug(f'Closing pos opened {position["date_bought"]}. Low is below sl. Loss:{-trade["profit"]}')
            position['remove'] = True
          elif candle.high >= position['tp']:
            trade = {
              'price_bought': position['price_bought'],
              'price_sold': position['tp'],
              'date_bought': position['date_bought'],
              'date_sold': candle.date
            }
            trade['profit'] = trade['price_sold'] - trade['price_bought']
            trade['profit_mlt'] = trade['profit']  / trade['price_bought']  + 1
            trade['r'] = (position['tp'] - trade['price_bought']) / (trade['price_bought'] - position['sl'])
            #print('sell tp')
            trades.append(trade)
            logging.debug(f'Closing pos opened {position["date_bought"]}. High is above tp. Profit:{trade["profit"]} ')
            position['remove'] = True
        for position in positions:
          if 'remove' in position:
            positions.remove(position)
  if bool(len(positions)): # Close any positions that have been left open
    print('Closing positions still open at the end:')
    for position in positions:
      logging.debug(f'Closing position opened {position["date_bought"]}')
      trade = {
        'price_bought': position['price_bought'],
        'price_sold': candleDf.iloc[-1]['close'],
        'date_bought': position['date_bought'],
        'date_sold': candleDf.iloc[-1]['date']
      }
      trade['profit'] = trade['price_sold'] - trade['price_bought']
      trade['profit_mlt'] = trade['profit']  / trade['price_bought'] + 1
      trade['r'] = (position['tp'] - trade['price_bought']) / (trade['price_bought'] - position['sl'])
      #print('sell tp')
      trades.append(trade)
  return trades

def getPerformanceReport(trades):
  def calcs(perf, trade):
    perf['profit'] = perf['profit'] + trade['profit']
    perf['profit_mlt'] = perf['profit_mlt'] * trade['profit_mlt']
    perf['num_wins'] += 1 if trade['profit'] > 0 else 0
    return perf
  if len(trades):
    performance = chainList(trades).reduce(calcs, {'profit': 0, 'profit_mlt': 1, 'num_wins': 0})
    performance['win_rate_pct'] = performance['num_wins'] / len(trades) * 100
    performance['num_trades'] = len(trades)
  else:
    performance = None
  return performance

def overallPerformaceReport(results):
  performance = {}
  for symbol_data in results.values():
    if 'profit' not in performance:
      performance['profit'] = symbol_data['performance']['profit']
      performance['profit_mlt'] = symbol_data['performance']['profit_mlt']
      performance['num_wins'] = symbol_data['performance']['num_wins']
      performance['num_trades'] = symbol_data['performance']['num_trades']
    else:
      performance['profit'] += symbol_data['performance']['profit']
      performance['profit_mlt'] *= symbol_data['performance']['profit_mlt']
      performance['num_wins'] += symbol_data['performance']['num_wins']
      performance['num_trades'] += symbol_data['performance']['num_trades']
  if 'num_trades' in performance: 
    performance['win_rate_pct'] = performance['num_wins'] / performance['num_trades'] * 100
  return pd.DataFrame([performance])

def backtestRunner(symbols, provider, candle_size, start_date, end_date, stratOpts):
  results = {}
  for symbol in symbols:
    print('\n', symbol)
    candleDf = getData(provider, symbol, candle_size, start_date, end_date)
    candleDf['engulfing'] = getBullishEngulfingCol(candleDf)
    print('ENGULFING CANDLES')
    print(candleDf[candleDf['engulfing']])
    trades = executeStrategy(candleDf, stratOpts)
    print('TRADES')
    print(pd.DataFrame(trades).to_string(index=False))
    performance = getPerformanceReport(trades)
    if performance != None:
      print('PERFORMANCE')
      print(pd.DataFrame([performance]).to_string(index=False))
      results[symbol] = {'df': candleDf, 'trades': trades, 'performance': performance}
  performance = overallPerformaceReport(results)
  print('\nOVERALL PERFORMANCE')
  print(performance.to_string(index=False))
  #print(results['EURUSD']['df'][results['EURUSD']['df']['engulfing'] == True])
  return performance

logging.basicConfig(level=logging.DEBUG)
provider = 'ib'
#symbol = 'EURUSD'
candle_size = '1D'
start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 11, 30)
tpR = 2
stratOpts = {
  'tpR': 6,
  'closeOrderAfter': 10,
  'retracementLevelPct': 50
}
#symbols = ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY']
symbols = ['USDJPY']
""" symbols = ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY',
  'AAPL', 'ADBE', 'AMGN', 'AMZN', 'BABA', 'BKNG', 'CHTR', 'CMCSA', 'COST', 'CSCO',
  'FB', 'FISV', 'GILD', 'GOOGL', 'INTC', 'INTU', 'MA', 'MDLZ', 'MSFT', 'NFLX', 'NVDA',
  'QCOM', 'SBUX', 'TXN', 'WMT',
  'ATVI', 'AMD', 'ALXN', 'ALGN', 'GOOG', 'ADI', 'ANSS', 'AMAT', 'ASML', 'ADSK',
  'ADP', 'BIDU', 'BIIB', 'BMRN', 'AVGO', 'CDNS', 'CDW', 'CERN', 'CHKP', 'CTAS', 'CTXS',
  'CTSH', 'CPRT', 'CSX',
  'DXCM', 'DOCU', 'DLTR', 'EBAY', 'EA', 'EXC', 'EXPE', 'FAST', 'FOX', 'FOXA',
  'IDXX', 'ILMN', 'INCY', 'ISRG', 'JD', 'KDP', 'KLAC', 'LRCX', 'LBTYK', 'LULU',
  'MAR', 'MXIM', 'MELI', 'MCHP', 'MRNA', 'MNST', 'NTES', 'NXPI', 'ORLY', 'PCAR',
  'PAYX', 'PYPL', 'PEP', 'PDD',
  'REGN', 'ROST', 'SGEN', 'SIRI', 'SWKS', 'SPLK', 'SNPS', 'TMUS', 'TTWO', 'TSLA',
  'KHC', 'TCOM', 'ULTA', 'VRSN', 'VRSK', 'VRTX', 'WBA', 'WDAY', 'XEL', 'XLNX', 'ZM'] """

candleDf = backtestRunner(symbols, provider, candle_size, start_date, end_date, stratOpts)
#chart(candleDf)


#candleDf = getData(provider, symbol, candle_size, start_date, end_date)
#candleDf['engulfing'] = getBullishEngulfingCol(candleDf)
#trades = executeStrategy(candleDf, tpR)
#performance = getPerformanceReport(trades)
""" 
provider = 'ib'
#symbol = 'EURUSD'
candle_size = '1D'
start_date = datetime(2020, 1, 1)
end_date = datetime(2020,2,1)
symbols = ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY',
    'AAPL', 'ADBE', 'AMGN', 'AMZN', 'BABA', 'BKNG', 'CHTR', 'CMCSA', 'COST', 'FB', 'FISV', 'GILD',
    'GOOGL', 'INTC', 'INTU', 'MA', 'MDLZ', 'MSFT', 'NFLX', 'NVDA', 'QCOM', 'SBUX', 'TXN', 'WMT']
#symbols = ['EURUSD']
overallPerformance = backtestRunner(symbols, provider, candle_size, start_date, end_date)
 """
