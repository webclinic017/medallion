#%%
from IbTrader import IbTrader
import sys
sys.path.append('../..')
from sqlite import sqliteDB, getTableName

#tickers = ['AAPL', 'ADBE', 'AMGN', 'AMZN', 'BABA', 'BKNG', 'CHTR', 'CMCSA', 'COST', 'CSCO', 'FB', 'FISV', 'GILD', 'GOOGL', 'INTC', 'INTU', 'MA', 'MDLZ', 'MSFT', 'NFLX', 'NVDA', 'QCOM', 'SBUX', 'TXN', 'WMT']
#tickers = ['EURUSD', 'USDJPY', 'GBPUSD', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD']
""" tickers = ['ATVI', 'AMD', 'ALXN', 'ALGN', 'GOOG', 'ADI', 'ANSS', 'AMAT', 'ASML', 'ADSK',
  'ADP', 'BIDU', 'BIIB', 'BMRN', 'AVGO', 'CDNS', 'CDW', 'CERN', 'CHKP', 'CTAS', 'CTXS',
  'CTSH', 'CPRT', 'CSX',
  'DXCM', 'DOCU', 'DLTR', 'EBAY', 'EA', 'EXC', 'EXPE', 'FAST', 'FOX', 'FOXA',
  'IDXX', 'ILMN', 'INCY', 'ISRG', 'JD', 'KDP', 'KLAC', 'LRCX', 'LBTYK', 'LULU',
  'MAR', 'MXIM', 'MELI', 'MCHP', 'MRNA', 'MNST', 'NTES', 'NXPI', 'ORLY', 'PCAR',
  'PAYX', 'PYPL', 'PEP', 'PDD',
  'REGN', 'ROST', 'SGEN', 'SIRI', 'SWKS', 'SPLK', 'SNPS', 'TMUS', 'TTWO', 'TSLA',
  'KHC', 'TCOM', 'ULTA', 'VRSN', 'VRSK', 'VRTX', 'WBA', 'WDAY', 'XEL', 'XLNX', 'ZM'] """
tickers = ['FB', 'MSFT', 'AMZN']
exchange = 'NASDAQ' # 'FX'
time_span = '1 Y'
interval = '15 mins'
endDateTime = '20201231 23:59:99'

ib = IbTrader()
ib.start()

db = sqliteDB()
db.connect()
for ticker in tickers:
  print('\n', ticker, ':')
  candleDf = ib.getHistoricalData(ticker, exchange, time_span, interval, endDateTime)
  if not candleDf.empty:
    db.insertCandleDfIntoDb(getTableName('ib', ticker, '15m'), candleDf)

db.disconnect()

#await asyncio.gather(*requests)
print('\nIMPORTED ALL REQUESTED DATA\n')

ib.stop()
print('\nDONE!\n')

 # %%
