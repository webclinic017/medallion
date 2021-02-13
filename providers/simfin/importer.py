#%% Simfin importer
from datetime import datetime, timedelta
from csv import reader
import time
import sys
sys.path.append('../..')
from sqlite import sqliteDB, getTableName
from providers.simfin.api import SimfinApi

def csvToList(filepath):
  with open(filepath, 'r') as read_obj:
    csv_reader = reader(read_obj, delimiter=';')
    return [k for k in csv_reader][1:]

def getVal(stmt, key):
  try:
    col_index = stmt['columns'].index(key)
    return stmt['data'][col_index]
  except:
    return None

def createFundamentalTableIfNotExists(db, table_name, stmt):
  sql = f'CREATE TABLE IF NOT EXISTS {table_name} ('
  for name, data in stmt.items():
    sql += f'{name} {data["type"]},'
  sql = sql[:-1] +  ')'
  c = db.conn.cursor()
  c.execute(sql)
  db.conn.commit()

def insertFundamentalStmt(db, table_name, stmt):
  sql = f'INSERT INTO {table_name} ('
  sql2 = ''
  for name in stmt:
    sql += f'{name},'
    sql2 += '?,'
  sql = sql[:-1]
  sql2 = sql2[:-1]
  sql += f') VALUES ({sql2})'
  data = tuple(map(lambda x: x['val'], stmt.values()))
  c = db.conn.cursor()
  try:
    c.execute(sql, data)
    db.conn.commit()
    return c.rowcount
  except:
    pass

def updateFundamentalStmt(db, table_name, quarter, net_income):
  sql = f'UPDATE {table_name} SET net_income = ? WHERE quarter = ?'
  c = db.conn.cursor()
  try:
    c.execute(sql, tuple([net_income, quarter]))
    db.conn.commit()
    return c.rowcount
  except:
    pass

def stmtQuarterExists(db, table_name, quarter):
  sql = f'SELECT * FROM {table_name} WHERE quarter = ?'
  print(sql)
  c = db.conn.cursor()
  c.execute(sql, tuple([quarter]))
  data = c.fetchall()
  return len(data) > 0

def upsertFundamentalIncomeSmt(db, table_name, quarter, stmt):
  if not stmtQuarterExists(db, table_name, quarter):
    data = {
      'quarter': quarter,
      'report_date': stmt[5],
      'publish_date': stmt[6],
      'earnings_per_share': None,
      'equity_per_share': None,
      'total_debt': None,
      'cash_cash_eq_short_inv': None,
      'minority_interest': None,
      'preferred_equity': None,
      'ebitda': None,
      'free_cash_flow': None,
      'net_income': stmt[26],
      'dividends_paid': None,
      'total_equity': None
    }
    insertFundamentalStmt(db, table_name, data)
  else:
    updateFundamentalStmt(db, table_name, quarter, stmt[26])

def importFundamentals(db, ticker, start_year=2010):
  # in db: row for each quarterly with all the necessary corresponding columns
  table_name = getTableName('simfin', ticker, 'fund')
  current_year = int(datetime.now().strftime('%Y'))
  quarters = ['q1', 'q2', 'q3', 'q4']
  for year in range(start_year, current_year):
    for q in quarters:
      stmt = simfin.getQuarterlyStatement(quarter=q, fyear=year, ticker=ticker)
      if 'error' in stmt:
        return stmt
      data = {
        'quarter': str(year) + '_' + q,
        'report_date': getVal(stmt, 'Report Date'),
        'publish_date': getVal(stmt, 'Publish Date'),
        'earnings_per_share': getVal(stmt, 'Earnings Per Share, Basic'),
        'equity_per_share': getVal(stmt, 'Equity Per Share'),
        'total_debt': getVal(stmt, 'Total Debt'),
        'cash_cash_eq_short_inv': getVal(stmt, 'Cash, Cash Equivalents & Short Term Investments'),
        'minority_interest': getVal(stmt, 'Minority Interest'),
        'preferred_equity': getVal(stmt, 'Preferred Equity'),
        'ebitda': getVal(stmt, 'EBITDA'),
        'free_cash_flow': getVal(stmt, 'Free Cash Flow'),
        'net_income': getVal(stmt, 'Net Income'),
        'dividends_paid': getVal(stmt, 'Dividends Paid'),
        'total_equity':  getVal(stmt, 'Total Equity')
      }
      print(ticker, year, q, 'Report Date:', data['report_date'])
      #print('Report Date:', data['report_date'])
      if data['report_date']:
        createFundamentalTableIfNotExists(db, table_name, data)
        insertFundamentalStmt(db, table_name, data)

def importCompanies():
  def createTableIfNotExists(db, table_name):
    sql = '''CREATE TABLE IF NOT EXISTS {} \
      (simfin_id int primary key, \
        ticker varchar(6), \
        company_name varchar(100), \
        industry_id int, \
        month_fy_end int, \
        num_employees int, \
        description varchar(300)
      )'''.format(table_name)
    c = db.conn.cursor()
    c.execute(sql)
    db.conn.commit()

  def insertCompany(db, table_name, company_info):
    sql = '''INSERT INTO {} (simfin_id, ticker, company_name, industry_id, month_fy_end, num_employees, \
      description) VALUES (?,?,?,?,?,?,?)'''.format(table_name)
    c = db.conn.cursor()
    try:
      c.execute(sql, tuple(company_info))
      db.conn.commit()
      return c.rowcount
    except:
      pass
  
  table_name = 'simfin_companies'
  db = sqliteDB()
  db.connect()
  createTableIfNotExists(db, table_name)
  companies = simfin.getListOfAllCompanies()
  for company in companies['data']:
    if (len(company) > 1 and company[1] > 'A'):
      print(company[1])
      company_info = simfin.getCompanyInfo(simfin_id=company[0])
      insertCompany(db, table_name, company_info[0]['data'])
  db.disconnect()

def importIndustries(filepath='/Users/knbo/Downloads/industries.csv'):
  def createTableIfNotExists(db, table_name):
    sql = '''CREATE TABLE IF NOT EXISTS {} \
      (industry_id int primary key, \
        sector varchar(100), \
        industry varchar(100)
      )'''.format(table_name)
    c = db.conn.cursor()
    c.execute(sql)
    db.conn.commit()

  def insertIndustry(db, table_name, industry_info):
    sql = '''INSERT INTO {} (industry_id, sector, industry) VALUES (?,?,?)'''.format(table_name)
    c = db.conn.cursor()
    try:
      c.execute(sql, tuple(industry_info))
      db.conn.commit()
      return c.rowcount
    except:
      pass

  table_name = 'simfin_industries'
  db = sqliteDB()
  db.connect()
  createTableIfNotExists(db, table_name)
  industries = csvToList(filepath)
  for industry in industries:
    ind = map(lambda x: x.replace('"', ''), industry[0].split(';'))
    insertIndustry(db, table_name, list(ind))
  db.disconnect()

def getCompanies(db):
  sql = '''SELECT * FROM simfin_companies'''
  c = db.conn.cursor()
  c.execute(sql)
  data = c.fetchall()
  return data

def importIncomeStmts(db, filepath='/Users/knbo/Downloads/us-income-quarterly.csv'):
  stmts = csvToList(filepath)
  for stmt in stmts:
    ticker = stmt[0]
    table_name = f'simfin_{ticker}_fund'
    createFundamentalTableIfNotExists(db, table_name, stmt)
    insertFundamentalStmt(db, table_name, stmt)

def importAllFundamentals(start_year=2000, ticker_start='A'):
  companies = getCompanies(db)
  for company in companies:
    if company[1] >= ticker_start and '.' not in company[1]:
      print('\nImporting fundamental data of', company[1])
      result = importFundamentals(db, company[1], start_year)
      if result and 'error' in result:
        break
  print('\nDone!')

def getCompanysLastStmtQuarter(db, table_name):
  sql = f'SELECT MAX(quarter) FROM {table_name}'
  c = db.conn.cursor()
  c.execute(sql)
  data = c.fetchone()
  db.conn.commit()
  return data[0]

def getCompanysStmtQuarters(db, table_name):
  sql = f'SELECT quarter FROM {table_name}'
  c = db.conn.cursor()
  c.execute(sql)
  data = c.fetchall()
  db.conn.commit()
  return list(map(lambda x: x[0], data))

def importMissingFundamentalStmts(db, simfin, ticker_start='A', last_quarter_to_check='2020_q3'):
  def getQuartersAvailable(availableStmts):
    available = {}
    for stmtType in ['pl', 'bs', 'cf']:
      for stmt in availableStmts[stmtType]:
        if stmt['period'][:1] == 'Q':
          quarter = f'{stmt["fyear"]}_{stmt["period"].lower()}'
          if quarter in available:
            available[quarter] = available[quarter] + 1
          else:
            available[quarter] = 1
    availList = []
    for quart, stmtCount in available.items():
      if stmtCount == 3:
        availList.append(quart)
    return availList
  
  def getMissingQuarters(availQuarters, gotQuarters):
    missing = []
    for availQuarter in availQuarters:
      if availQuarter not in gotQuarters:
        missing.append(availQuarter)
    return missing

  companies = getCompanies(db)
  for comp_num, company in enumerate(companies):
    simfin_id = company[0]
    ticker = company[1]
    print('\nTicker:', f'{comp_num+1}/{len(companies)}', f'{int((comp_num+1)/len(companies)*100)}%', ticker)
    if ticker >= ticker_start and '.' not in ticker and '_' not in ticker and '-' not in ticker:
      table_name = 'simfin_' + ticker +'_fund'
      table_exists = db.tableExists(table_name)
      lastStmtQuarter = getCompanysLastStmtQuarter(db, table_name) if table_exists else None
      if not lastStmtQuarter or lastStmtQuarter < last_quarter_to_check:
        availStmts =  simfin.getCompanysAvailableStmts(simfin_id)
        if 'error' in availStmts:
          return
        availQuarters = getQuartersAvailable(availStmts)
        gotQuarters = getCompanysStmtQuarters(db, table_name) if table_exists else []
        missingQuarters = getMissingQuarters(availQuarters, gotQuarters)
        for quarter in missingQuarters:
          quarterSplit = quarter.split('_')
          stmt = simfin.getQuarterlyStatement(quarter=quarterSplit[1], fyear=quarterSplit[0], ticker=ticker)
          if 'error' in stmt:
            return stmt
          #print('Report Date:', data['report_date'])
          if 'report_date' in stmt:
            print(ticker, quarter, 'Report Date:', stmt['report_date']['val'])
            createFundamentalTableIfNotExists(db, table_name, stmt)
            insertFundamentalStmt(db, table_name, stmt)


#%%
start = time.time()
db = sqliteDB()
db.connect()
simfin = SimfinApi()
#%%
#importFundamentals(db, ticker, start_year=2010)
#importAllFundamentals(start_year=2000, ticker_start='ABMD')
importMissingFundamentalStmts(db, simfin)
#%%
db.disconnect()
total_secs = time.time() - start
print('\nTime taken:', int(total_secs/60), 'mins', int(total_secs%60), 'secs')
#%%
