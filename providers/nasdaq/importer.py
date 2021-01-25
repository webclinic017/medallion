# Update tickers in db

#%%
import sqlite3
from get_tickers import getUsExchangeTickers
import sys
sys.path.append('../..')
from sqlite import sqliteDB

db = sqliteDB()
db.connect()

tickers = getUsExchangeTickers()
db.createTickerTableIfNotExists('nasdaq_tickers')
for ticker in tickers:
  if not db.tickerInNasdaqTickerTable(ticker['symbol']):
    db.addTickerToNasdaqTickerTable(ticker)

db.disconnect()

# %%
