#%% IMPORTS
from datetime import timedelta
import pandas as pd
import sys
sys.path.append('..')
from sqlite import sqliteDB, getTableName
from backtesting.FakeTrader import FakeTrader
from reporter.Reporter import Reporter
from reporter.Plotter import Plotter

# %%
'''
This backtest runner doesn't specify a symbol for each iteration
It needs to access all symbols inside each iteration
Initally adapted for the price target strategy
'''
def backtestRunner(provider, candle_size, start_date, end_date, init_balance, stratOpts):
  delta = {
      '1m': timedelta(minutes = 1),
      '5m': timedelta(minutes = 5),
      '15m': timedelta(minutes = 15),
      '1h': timedelta(hours = 1),
      '4h': timedelta(hours = 4),
      '1D': timedelta(days = 1)
    }[candle_size]
  current_datetime = start_date
  db = sqliteDB()
  db.connect()

  # Setup. Get warm up candles:
  data = {}
  trader = FakeTrader(init_balance)
  data['price_target'] = {}
  data['price_target']['candles'] = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'price_target'])
  trader.trades['price_target'] = []

  plotter = Plotter(['price_target'])
  #plotter.createLine('price_target', 'price_target', 'green')

  pos_size = stratOpts['pos_size']
  
  symbols = db.getKoyfinTickers()
  # strategy = Strategy(stratOpts, trader, plotter, symbols)
  while current_datetime < end_date:
    print(f'{current_datetime.strftime("%Y-%m-%d")}')
    for symbol in symbols:
      table_name = getTableName(provider, symbol, '1D')
      candle = db.getCandle(table_name, current_datetime)
      data['price_target']['candles'] = data['price_target']['candles'].append(candle, ignore_index=True)
      #plotter.addPtToLine('price_target', 'price_target', candle['date'], candle['price_target'])
      if candle == None or candle['price_target'] == None:
        # No enough data for this day
        pass
      else:
        # if  
        price_dif_pct = (candle['price_target'] - candle['close']) / candle['close'] * 100
        if price_dif_pct > 30 and not trader.isPosOpen(symbol):
          order = {
            'symbol': symbol,
            'quantity': pos_size / candle['close'],
            'mkt_price': candle['close'],
            'placed': candle['date']
          }
          trader.openPosition(order, current_datetime)
          print('buy', symbol, 'price:', candle['close'], 'target:', candle['price_target'], 'price dif:', price_dif_pct)
        for order in trader.orders:
          if order['status'] == 'active' and order['symbol'] == symbol and price_dif_pct < 20:
            trader.closePosition(order, candle['close'], current_datetime)
            print('sell', symbol, 'price dif:', price_dif_pct)
    current_datetime += delta

  reporter = Reporter(['price_target'], plotter, init_balance)

  reporter.setCandles('price_target', data['price_target']['candles'])
  reporter.setTrades('price_target', trader.trades['price_target'], trader.balance, trader.num_trades, trader.num_wins, trader.total_r)

  db.disconnect()
  
  # profit = trader.balance - init_balance
  # profitPct = profit / init_balance * 100
  # winRate = trader.num_wins / trader.num_trades * 100
  # print('\nFinal balance: {:.2f} Profit: {:.2f} Profit%: {:.2f}%'.format(trader.balance, profit, profitPct))
  # print('Num trades: {} Num wins: {} Win rate: {:.2f}%\n'.format(trader.num_trades, trader.num_wins, winRate))

  # TODO: Return a reporter class instance that includes functions such as trades_to_excel, show_chart, chart_to_html, print_performance, etc.
  # The strategy needs access to the plotter class in order to add lines and dots
  # The dots that correspond to trades should be added automatically
  return reporter
  