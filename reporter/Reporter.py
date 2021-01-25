from pathlib import Path
import pandas as pd
import sys
sys.path.append('..')
from reporter.Chart import Chart

class Reporter():
  def __init__(self, symbols: list, plotter, init_balance):
    self.data = {}
    for symbol in symbols:
      self.data[symbol] = {
        'lines_to_plot': plotter.data[symbol]['lines'],
        'scatter_pts_to_plot': plotter.data[symbol]['scatter_pts'],
        'init_balance': init_balance
      }

  def setCandles(self, symbol: str, candles: pd.DataFrame):
    self.data[symbol]['candles'] = candles

  def setTrades(self, symbol:str, trades: list, balance: float, num_trades: int, num_wins: int, total_r: float):
    self.data[symbol]['trades'] = trades
    self.data[symbol]['balance'] = balance
    self.data[symbol]['num_trades'] = num_trades
    self.data[symbol]['num_wins'] = num_wins
    self.data[symbol]['total_r'] = total_r

  def getTrades(self, symbol):
    return self.data[symbol]['trades']

  def printResults(self, symbol: str):
    init_balance = self.data[symbol]['init_balance']
    balance = self.data[symbol]['balance']
    num_wins = self.data[symbol]['num_wins']
    num_trades = self.data[symbol]['num_trades']
    total_r = self.data[symbol]['total_r']
    avg_r = (total_r / num_trades if num_trades > 0 else 0) if total_r else None
    profit = balance - init_balance
    profit_pct = profit / init_balance * 100
    win_rate = num_wins / num_trades * 100 if num_trades > 0 else None
    print('\nFinal balance: {:.2f} Profit: {:.2f} Profit%: {:.2f}%'.format(balance, profit, profit_pct))
    if num_trades > 0:
      if total_r:
        print('Num trades: {} Num wins: {} Win rate: {:.2f}% Total R:{:.2f} Avg R:{:.2f}\n'.format(num_trades, num_wins, win_rate, total_r, avg_r))
      else:
        print('Num trades: {} Num wins: {} Win rate: {:.2f}%\n'.format(num_trades, num_wins, win_rate))
    else:
      print('No trades were made')

  def printTrades(self, symbol:str):
    print(symbol, 'TRADES:')
    print(pd.DataFrame(self.data[symbol]['trades']).to_string(index=False))

  def exportTradesToExcel(self, symbol: str, filename: str):
    """Writes to the filename given and to the sheet with name that coincides with the symbol"""
    filename = f'{filename}-{symbol}.xlsx'
    if Path(filename).exists():
      with pd.ExcelWriter(filename, mode='a') as writer:
        self.data[symbol]['trades'].to_excel(writer, sheet_name=symbol)
    else:
      with pd.ExcelWriter(filename, mode='w') as writer:
        self.data[symbol]['trades'].to_excel(writer, sheet_name=symbol)
  
  def getChart(self, symbol: str):
    scatter_pts = self.data[symbol]['scatter_pts_to_plot']
    scatter_pts.append({'name': 'Buy', 'color': 'green', 'symbol': 'circle', 'x': [], 'y': []})
    scatter_pts.append({'name': 'Sell', 'color': 'red', 'symbol': 'circle', 'x': [], 'y': []})
    for trade in self.data[symbol]['trades']:
      for scatter_gp in scatter_pts:
        if scatter_gp['name'] == 'Buy':
          scatter_gp['x'].append(trade['time_opened'])
          scatter_gp['y'].append(trade['price_bought'])
        elif scatter_gp['name'] == 'Sell':
          scatter_gp['x'].append(trade['time_closed'])
          scatter_gp['y'].append(trade['price_sold'])

    return Chart(
        self.data[symbol]['candles'],
        self.data[symbol]['scatter_pts_to_plot'],
        self.data[symbol]['lines_to_plot'],
        width=800, height=600, margin=dict(l=50, r=50, b=100, t=100, pad=4)
      )