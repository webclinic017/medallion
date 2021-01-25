# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from BiasBarStrategy import BiasBarStrategy
import sys
sys.path.append('..')
#from providers.ib.IbTrader import IbTrader
from backtesting.runner import backtestRunner


# Run Back Test...
logging.basicConfig(level=logging.ERROR)
provider = 'ib'
candle_size = '1D'
start_date = datetime(2018, 1, 1)
end_date = datetime(2018, 12, 31)
init_balance = 10000
pos_size = 1000
stratOpts = {
  'tpR': 5,
  'closeOrderAfter': 10,
  'retracementLevelPct': 50,
  'pos_prop': 0.1,
  'smaSize': 25 # If 0 SMA not applied
}
stratOpts['numWarmUpCandles'] = stratOpts['smaSize']
#symbols = ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY']
symbol = 'AUDUSD'
symbols = [symbol]
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
  'KHC', 'TCOM', 'ULTA', 'VRSN', 'VRSK', 'VRTX', 'WBA', 'WDAY', 'XEL', 'XLNX', 'ZM']
 """
reporter = backtestRunner(BiasBarStrategy, symbols, provider, candle_size, start_date, end_date, init_balance, stratOpts)
reporter.printResults('AUDUSD')
#%%
'''
print('TRADES:')
for symbol in symbols:
  print('\n', symbol)
  print(pd.DataFrame(data[symbol]['trades']).to_string(index=False))
  trades = pd.DataFrame(data[symbol]['trades']).apply(lambda row:  [
      datetime.strftime(row.date_bought, '%Y-%m-%d'),
      row.price_bought,
      row.sl,
      row.tp,
      row.diff,
      row.price_sold,
      datetime.strftime(row.date_sold, '%Y-%m-%d'),
      row.profit
    ], axis=1, result_type='broadcast')
  del trades['diff']
  filename = 'BiasBarStrategy.xlsx'
  if Path(filename).exists():
    with pd.ExcelWriter(filename, mode='a') as writer:
      trades.to_excel(writer, sheet_name=symbol)
  else:
    with pd.ExcelWriter(filename, mode='w') as writer:
      trades.to_excel(writer, sheet_name=symbol)
'''

  #trades.to_excel('AUDUSD.xlsx', sheet_name='AUDUSD2', index=False)
#  profit = 0
#  num_wins = 0
#  for trade in data[symbol]['trades']:
#    try:
#      final_pm
#      final_pm *= trade['profit_mlt']
#    except NameError:
#      final_pm = trade['profit_mlt']
#    profit += trade['profit']
#    num_wins += 1 if trade['profit'] > 0 else 0
#  print('Num trades:', len(data[symbol]['trades']), 'Num wins:', num_wins, 'Profit_mlt:', final_pm, 'Profit:', profit)

# %%
# Plotter
#symbol = 'NFLX'
""" candles = data[symbol]['candles']
candles['SMA'] = candles['close'].rolling(stratOpts['smaSize']).mean()
scatterDots = [
    {
      'x': [eng['date'] for eng in data[symbol]['engulfings']],
      'y': [eng['y'] for eng in data[symbol]['engulfings']],
      'name': 'Engulfing',
      'symbol': 'cross',
      'color': 'blue'
    },nnbb 
    {
      'x': [eng['date_bought'] for eng in data[symbol]['trades']],
      'y': [eng['price_bought'] for eng in data[symbol]['trades']],
      'name': 'Buy',
      'symbol': 'circle',
      'color': 'green'
    },
    {
      'x': [eng['date_sold'] for eng in data[symbol]['trades']],
      'y': [eng['price_sold'] for eng in data[symbol]['trades']],
      'name': 'Sell',
      'symbol': 'circle',
      'color': 'red'
    }
  ]

lines = [
  {
    'name': 'SMA',
    'x': candles['date'].tolist(),
    'y': candles['SMA'].tolist(),
    'color': 'orange'
  }
]
write_html = chart(data[symbol]['candles'], scatterDots, lines, width=1600, height=800, margin=dict(l=50, r=50, b=50, t=50, pad=4))
write_html('BiasBarStrat_' + symbol + '_2015-2020.html')
 """
# %%
