#%% sqlite
import sqlite3
from pathlib import Path
import os
import pandas as pd
from datetime import datetime
  
def getTableName(provider, symbol, candle_size):
  return f'{provider}_{symbol}_{candle_size}'

class sqliteDB():
  def connect(self):
    db_file_path = os.fspath(Path.home()) + f'/.medallion/medallion.db'
    self.conn = sqlite3.connect(db_file_path)

  def disconnect(self):
    self.conn.close()

  def createCandleTableIfNotExists(self, table_name):
    sql = '''CREATE TABLE IF NOT EXISTS {} \
(time timestamp primary key,\
open real(15,5), high real(15,5), low real(15,5), close real(15,5), \
volume integer, price_target real(15,5), num_analysts integer\
)'''.format(table_name)
    c = self.conn.cursor()
    c.execute(sql)
    self.conn.commit()

  def tableExists(self, table_name):
    sql = f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';'''
    c = self.conn.cursor()
    c.execute(sql)
    data = c.fetchall()
    return len(data) == 1

  def createTickerTableIfNotExists(self, table_name):
    sql = f'''CREATE TABLE IF NOT EXISTS {table_name} \
(ticker_id integer primary key,\
symbol varchar(20), name varchar(150), exchange varchar(20), market_cap real(15,5),\
country varchar(150), ipoyear real(15,5), industry varchar(150), \
sector varchar(150), url varchar(150)
)'''
    c = self.conn.cursor()
    c.execute(sql)
    self.conn.commit()

  def tickerInNasdaqTickerTable(self, symbol: str, exchange: str = None):
    if exchange == None:
      sql = '''SELECT COUNT(ticker_id) FROM nasdaq_tickers WHERE symbol = "{}"'''.format(symbol)
      c =  self.conn.cursor()
      c.execute(sql)
    else:
      sql = '''SELECT COUNT(ticker_id) FROM nasdaq_tickers WHERE symbol = "{}" and exchange = "{}"'''.format(symbol, exchange)
      c =  self.conn.cursor()
      c.execute(sql)
    data = c.fetchall()
    return data[0][0] != 0
  
  def addTickerToNasdaqTickerTable(self, ticker_info):
    sql = '''INSERT INTO nasdaq_tickers (symbol, name, exchange, market_cap, country, ipoyear, industry, sector, url) \
      VALUES(?,?,?,?,?,?,?,?,?)'''
    c = self.conn.cursor()
    c.execute(sql, (ticker_info['symbol'], ticker_info['name'], ticker_info['exchange'], ticker_info['marketCap'], ticker_info['country'], ticker_info['ipoyear'], ticker_info['industry'], ticker_info['sector'], ticker_info['url']))
    self.conn.commit()
    return c.rowcount

  def insertCandle(self, table_name, time, open, high, low, close, volume, price_target=None, num_analysts=None):
    sql = '''INSERT INTO {} (time, open, high, low, close, volume, price_target, num_analysts) \
VALUES(?,?,?,?,?,?,?,?)'''.format(table_name)
    c = self.conn.cursor()
    c.execute(sql, (time, open, high, low, close, volume, price_target, num_analysts))
    self.conn.commit()
    return c.rowcount

  def getCandles(self, table_name, start_date, end_date):
    sql = '''SELECT * FROM {} WHERE time >= ? AND time < ?'''.format(table_name)
    c = self.conn.cursor()
    c.execute(sql, (start_date, end_date))
    data = c.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'price_target', 'num_analysts'])
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    return df

  ############################
  ### High Level Functions ###
  ############################
  def insertCandleDfIntoDb(self, table_name, candleDf):
    self.createCandleTableIfNotExists(table_name)
    num_candles = len(candleDf.index)
    inserted = 0
    for row in range(num_candles):
      cnd = candleDf.iloc[row].to_list()
      dt_date = datetime.strptime(cnd[0], '%Y%m%d') if len(cnd[0]) == 8 else datetime.strptime(cnd[0], '%Y%m%d %H:%M:%S')
      try:
        candles_inserted = self.insertCandle(table_name, dt_date, cnd[1], cnd[2], cnd[3], cnd[4], cnd[5], cnd[6] if 6 in cnd else None)
        inserted += candles_inserted
      except Exception as e:
        print(e, 'FOR TIME', cnd[0])
    print('Inserted', inserted, 'candles', table_name, 'in the DB.', num_candles, 'were candles processed')

  def getLastCandles(self, table_name, latest_date, num_candles):
    "Gets the specified number of latest candles"
    sql = '''SELECT * FROM {} WHERE time < ? ORDER BY time DESC LIMIT ?'''.format(table_name)
    c = self.conn.cursor()
    c.execute(sql, (latest_date, num_candles))
    data = c.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'price_target', 'num_analysts'])
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    return df

  def getLastCandleTime(self, table_name):
    sql = f'''SELECT MAX(time) FROM {table_name}'''
    c = self.conn.cursor()
    c.execute(sql)
    data = c.fetchall()
    try:
      return datetime.strptime(data[0][0], '%Y-%m-%d %H:%M:%S')
    except:
      print('Could not get last candle time for table', table_name, 'Got this:', data)

  def getCandle(self, table_name, date):
    "Returns the candle of the specified date as a dict, if there is no candle on that date, it returns None"
    sql = '''SELECT * FROM {} WHERE time = ?'''.format(table_name)
    c = self.conn.cursor()
    try:
      c.execute(sql, (date,))
      data = c.fetchone()
      if data == None:
        return None
      else:
        returnedDate  = datetime.strptime(data[0], '%Y-%m-%d %H:%M:%S')
        return {'date': returnedDate, 'open': data[1], 'high': data[2], 'low': data[3], 'close': data[4], 'volume': data[5], 'price_target': data[6], 'num_analysts': data[7]}
    except:
      return None
    
  def getAllNasdaqTickers(self):
    sql = f'SELECT symbol FROM nasdaq_tickers'
    c = self.conn.cursor()
    c.execute(sql)
    data = c.fetchall()
    tickers = []
    for result in data:
      tickers.append(result[0])
    return tickers
  
  def getKoyfinTickers(self):
    sql = 'select distinct tbl_name from sqlite_master where tbl_name like "%koyfin%"'
    c = self.conn.cursor()
    c.execute(sql)
    data = c.fetchall()
    tickers = []
    for result in data:
      tickers.append(result[0].replace('koyfin_', '').replace('_1D', ''))
    return tickers

  def dropAllKoyfinTables(self):
    sql = f'SELECT tbl_name FROM sqlite_master WHERE tbl_name LIKE "%koyfin%"'
    c = self.conn.cursor()
    c.execute(sql)
    table_list = c.fetchall()
    for table in table_list:
      sql = f'DROP TABLE IF EXISTS "{table[0]}"'
      c = self.conn.cursor()
      c.execute(sql)
      self.conn.commit()
      print('Dropped', table[0])
    print('Done!')
    

db = sqliteDB()
db.connect()
db.getKoyfinTickers()
db.disconnect()
# %%
