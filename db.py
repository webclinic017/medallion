#%%
import mysql.connector
from util import chainList
from functools import reduce

db = None

# %%
def connect():
  global db
  db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sqlForest",
    database="securities",
    auth_plugin='mysql_native_password'
  )

#%%
def addTicker(ticker, name, exchangeCode=None, description=None, asset_type=None, ipo_date=None, delisting_date=None, status=None, sector=None, industry=None):
  print('Sector:', sector, 'Industry:', industry)
  desc = '' if description == None else f', "{description}"'
  assetType = '' if asset_type == None else f', "{asset_type}"'
  ipoDate = '' if ipo_date == None else f', "{ipo_date}"'
  delistingDate = '' if delisting_date == None else f', "{delisting_date}"'
  status = '' if status == None else f', "{status}"'
  sector = '' if sector == None else f', "{sector}"'
  industry = '' if industry == None else f', "{industry}"'
  descField = ', description' if bool(description) else ''
  assetTypeField = ', asset_type' if bool(asset_type) else ''
  ipoDateField = ', ipo_date' if bool(ipo_date) else ''
  delistingDateField = ', delisting_date' if bool(delisting_date) else ''
  sectorField = ', sector' if bool(sector) else ''
  industryField = ', industry' if bool(industry) else ''
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  if exchangeCode == None:
    sql = f'INSERT INTO ticker(ticker, name{descField}{assetTypeField}{ipoDateField}{delistingDateField}{sectorField}{industryField}) '\
      + f'VALUES ("{ticker}", "{name}"{desc}{assetType}{ipoDate}{delistingDate}{status}{sector}{industry});'
  else:
    sql = f'INSERT INTO ticker (exchange_id, ticker, name, description, asset_type, ipo_date, delisting_date, status, sector, industry) '\
      + f'SELECT exchange.id, "{ticker}", "{name}", "{desc}", "{asset_type}", "{ipo_date}", "{delisting_date}", "{status}", "{sector}", "{industry}" '\
      + f'FROM exchange WHERE exchange.code="{exchangeCode}";'
  print(sql)
  cursor.execute(sql)
  db.commit()
  return cursor.rowcount

def addSp500Tickers(tickers):
  rowCount = 0
  for stock in tickers['stockList']:
    rowCount += addTicker(exchangeCode=stock[2], ticker=stock[0], name=stock[1], description=None, asset_type=stock[3], ipo_date=stock[4], delisting_date='NULL' if stock[5] == 'null' else f'"{stock[5]}"', status=stock[6], sector=None, industry=None)
  return rowCount

def addDataVendor(name, website_url = ''):
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  sql = "INSERT INTO data_vendor (name, website_url) VALUES (%s, %s)"
  val = (name, website_url)
  cursor.execute(sql, val)
  db.commit()
  return cursor.lastrowid

def getAllTickerCodes():
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  sql = 'SELECT ticker FROM ticker'
  cursor.execute(sql)
  result = cursor.fetchall()
  return chainList(result).map(lambda t: t[0])

def getTickerId(ticker):
  global db
  if db == None:
    connect()
  cursor = db.cursor(buffered=True)
  sql = "SELECT id FROM ticker WHERE ticker ='" + ticker + "'"
  cursor.execute(sql)
  result = cursor.fetchone()
  if result == None:
    return None
  return result[0]

def getDataVendorId(name):
  global db
  if db == None:
    connect()
  cursor = db.cursor(buffered=True)
  sql = "SELECT id FROM data_vendor WHERE name ='" + name + "'"
  cursor.execute(sql)
  result = cursor.fetchone()
  if result == None:
    return None
  return result[0]

def getExchangeId(code):
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  sql = "SELECT id FROM exchange WHERE code ='" + code + "'"
  cursor.execute(sql)
  result = cursor.fetchone()
  if result == None:
    return None
  return result[0]

def getTickerPrices(ticker):
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  sql = f'SELECT price_date, open, high, low, close, volume FROM daily_price INNER JOIN ticker ON daily_price.ticker_id = ticker.id WHERE ticker.ticker="{ticker}";'
  cursor.execute(sql)
  result = cursor.fetchall()
  return result

def addPrice(data_vendor, ticker, price_date, open = '', close = '', high = '', low = '', adj_close = '', volume = ''):
  # get data_vendor_id and ticker_id
  data_vendor_id = getDataVendorId(data_vendor)
  ticker_id = getTickerId(ticker)
  if data_vendor_id == None:
    data_vendor_id = addDataVendor(name=data_vendor)
  if ticker_id == None:
    ticker_id = addTicker(ticker)
  global db
  if db == None:
    connect()
  cursor = db.cursor()
  sql = "INSERT INTO daily_price (data_vendor_id, ticker_id, price_date, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
  val = (data_vendor_id, ticker_id, price_date, open, close, high, low, adj_close, volume)
  cursor.execute(sql, val)
  db.commit()
  return cursor.rowcount

def addPrices(data_vendor, ticker, prices):
  # get data_vendor_id and ticker_id
  data_vendor_id = getDataVendorId(data_vendor)
  ticker_id = getTickerId(ticker)
  if data_vendor_id == None:
    raise Exception('Data vendor', data_vendor, 'not found in DB')
  if ticker_id == None:
    raise Exception('Ticker', ticker, 'not found in DB')
  val = tuple(map(lambda price: (data_vendor_id, ticker_id, price[0], price[1], price[2], price[3], price[4], price[5], price[6]), prices))
  global db
  if db == None:
    connect()
  cursor = db.cursor(buffered=True)
  sql = "INSERT INTO daily_price (data_vendor_id, ticker_id, price_date, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
  cursor.executemany(sql, val)
  db.commit()
  return cursor.rowcount

# %%
#connect()
# addTicker('FB')
# addDataVendor('alphavantage')
#result = addDataVendor('bitfinex')
#print(result)

#  print('Loading prices for', ticker, '...')
#  dailyPrices = getTimeSeriesDaily(ticker)
#  rowsAdded = addKPrices('alphavantage', ticker, dailyPrices['prices'])
#  print(ticker, ': Added ', rowsAdded, ' candles to DB')

