#%% IMPORTS
from datetime import timedelta
import sys
sys.path.append('..')
from sqlite import sqliteDB, getTableName
from backtesting.FakeTrader import FakeTrader
from reporter.Reporter import Reporter
from reporter.Plotter import Plotter

# %%
'''
This backtest runner uses a list of symbols and runs on_candle on_end etc. for each symbol on every candle
'''
def backtestRunner(Strategy, symbols, provider, candle_size, start_date, end_date, init_balance, stratOpts):
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
  def getTable(symbol):
    return getTableName(provider, symbol, candle_size)

  # Setup. Get warm up candles:
  data = {}
  trader = FakeTrader(init_balance)
  for symbol in symbols:
    data[symbol] = {}
    data[symbol]['candles'] = db.getLastCandles(getTable(symbol), start_date, stratOpts['numWarmUpCandles'])
    trader.trades[symbol] = []

  plotter = Plotter(symbols)
  
  strategy = Strategy(stratOpts, trader, plotter, symbols)
  while current_datetime < end_date:
    #print(f'    {current_date.strftime("%Y-%m-%d")}')
    for symbol in symbols:
        candle = db.getCandle(getTable(symbol), current_datetime)
        if candle == None:
          #print('Not a trading day')
          pass
        else:
          #trader.checkPendingOrders(symbol, data[symbol]['candles'].iloc[-1], stratOpts['closeOrderAfter'])
          data[symbol]['candles'] = data[symbol]['candles'].append(candle, ignore_index=True)
          #logging.debug(f'appended to candles:\n{candles[symbol]}')
          if len(data[symbol]['candles'].index) > stratOpts['numWarmUpCandles']:
            strategy.on_candle(symbol, data[symbol]['candles'], db)
    #print('\n', datetime.strftime(current_date, '%Y-%m-%d'), symbol, candles[symbol])
    current_datetime += delta

  reporter = Reporter(symbols, plotter, init_balance)
  for symbol in symbols:
    strategy.on_end(symbol, data[symbol]['candles'])
    reporter.setCandles(symbol, data[symbol]['candles'])
    reporter.setTrades(symbol, trader.trades[symbol], trader.balance, trader.num_trades, trader.num_wins, trader.total_r)

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