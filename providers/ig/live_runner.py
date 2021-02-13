#%% IMPORTS
import threading
import json
from datetime import datetime, timedelta
from ig_trader import IgTrader
import sys
sys.path.append('../..')
from reporter.Reporter import Reporter
from reporter.Plotter import Plotter

def log_file(txt):
  with open('log.txt', 'a') as f:
    f.write(txt)

# %%
class LiveRunner():
  def __init__(self, Strategy, symbol, provider, candle_size, strat_opts, stop_time):
    self.interval = {
        '5m': '5MINUTE'
      }[candle_size]
    self.symbol = symbol
    self.provider = provider
    self.strat_opts = strat_opts
    self.trader = IgTrader()
    self.plotter = Plotter([symbol])
    self.strategy = Strategy(strat_opts, self.trader, self.plotter, symbol)
    self.reporter_started = False
    self.candles: list[dict] =  []
    self.pos_size = strat_opts['pos_size']
    self.stop_time = stop_time
    self.ev_finshed = threading.Event()

  def run(self):
    self.strategy_thread = threading.Thread(name='Strategy_Runner', target=self.run_strategy)
    self.strategy_thread.start()
    self.ev_finshed.wait()

  def run_strategy(self):
    self.subscriptions = []
    self.trader.connect()
    self.subscriptions.append(self.trader.subscribeToAccountStatus(self.onAccountUpdate))
    self.subscriptions.append(self.trader.subscribeToTradeUpdates(self.onTradeUpdate))
    self.subscriptions.append(self.trader.subscribeToCandleData([self.symbol], self.interval, self.onCandle))
    self.subscriptions.append(self.trader.subscribeToTickData([self.symbol], self.onTick))
  
  def stop_strategy(self):
    print('stop strategy')
    for subscription in self.subscriptions:
      print('Unsubscribe')
      self.trader.unsubscribe(subscription)
    self.ev_finshed.set()

  def onAccountUpdate(self, data):
    vals = data['values']
    print(f"ACCOUNT UPDATE: {vals}")
    self.available_cash = vals['AVAILABLE_CASH']
    self.profit = vals['PNL']
    if not self.reporter_started:
      self.reporter = Reporter([self.symbol], self.plotter, init_balance=self.available_cash)
      self.reporter_started = True

  def onTradeUpdate(self, data):
    vals = data['values']
    print(f"TRADE UPDATE: {vals}")
    if 'CONFIRMS' in vals and vals['CONFIRMS'] != None:
      confirmation = json.loads(vals['CONFIRMS'])
      if confirmation['dealStatus'] == 'REJECTED':
        print(f"TRADE REJECTED. REASON: {confirmation['reason']}")
    if 'OPU' in vals and vals['OPU'] != None:
      open_pos = json.loads(vals['OPU'])
      if open_pos['status'] == 'OPEN':
        pos = {'deal_ref': open_pos['dealReference'], 'deal_id': open_pos['dealId'], 'epic': open_pos['epic'],
           'direction': open_pos['direction'], 'level': open_pos['level'], 'size': open_pos['size'], 'time': open_pos['timestamp']}
        to_confirm_idx = None
        for i, to_confirm in enumerate(self.trader.pos_to_confirm):
          if pos['deal_ref'] == to_confirm['deal_ref']:
            to_confirm_idx = i
            break
        if 'tr_sl' in self.trader.pos_to_confirm[to_confirm_idx]:
          pos['tr_sl'] = self.trader.pos_to_confirm[to_confirm_idx]['tr_sl']
          pos['tr_sl_dist'] = self.trader.pos_to_confirm[to_confirm_idx]['tr_sl_dist']
        if 'tp' in self.trader.pos_to_confirm[to_confirm_idx]:
          pos['tp'] = self.trader.pos_to_confirm[to_confirm_idx]['tp']
          pos['tp_dist'] = self.trader.pos_to_confirm[to_confirm_idx]['tp']
        self.trader.positions.append(pos)
        self.trader.pos_to_confirm.pop(to_confirm_idx)
        record = f"POSITION OPENED. Deal Id: {pos['deal_id']}; Level: {pos['level']}; Direction: {pos['direction']}; Size: {pos['size']}; Time: {pos['time']}"
        print(record)
        log_file(record + '\n')
        self.strategy.onPositionOpen(pos)
      if open_pos['status'] == 'DELETED':
        pos = {'deal_ref': open_pos['dealReference'], 'deal_id': open_pos['dealId'], 'epic': open_pos['epic'],
           'direction': open_pos['direction'], 'level': open_pos['level'], 'size': open_pos['size'], 'time': open_pos['timestamp']}
        idx = None
        for i, open_position in enumerate(self.trader.positions):
          if open_position['deal_ref'] == pos['deal_ref']:
            idx = i
            break
        self.trader.positions.pop(idx)
        record = f"POSITION CLOSED. Deal Id: {pos['deal_id']}; Level: {pos['level']}; Direction: {pos['direction']}; Time: {pos['time']}"
        print(record)
        log_file(record + '\n')
        self.strategy.onPositionClosed(pos)
  
  def onCandle(self, data):
    if data['values']['CONS_END'] == 1:
      candle = data['values']
      candle_time = datetime.fromtimestamp(int(candle['UTM'])/1000)
      print(f"CANDLE UPDATE: {candle_time} {candle}")
      mins = int(candle_time.strftime('%M'))
      if mins == 59:
        self.trader.login()
      self.candles.append(candle)
      self.strategy.onCandle(candle)

  def onTick(self, data):
    tick = data['values']
    #print(f"TICK: {tick}")
    tick_time = datetime.fromtimestamp(int(tick['UTM'])/1000)
    #print(f"TIME: {tick_time.strftime('%H:%M:%S')} {self.stop_time.strftime('%H:%M:%S')}")
    if tick_time > self.stop_time:
      self.stop_strategy()
    for position in self.trader.positions:
      if position['direction'] == 'BUY':
        # Check to see if it is time to close a long position
        if tick['BID'] < position['tr_sl']:
          close_pos = {
            'dealId': position['deal_id'],
            'direction': 'SELL',
            'orderType': 'MARKET',
            'size': position['size']
          }
          self.trader.closePosition(close_pos)
        # if long position not to close, update sl if necessary
        else:
          new_long_sl = tick['BID'] - position['tr_sl_dist']
          if new_long_sl > position['tr_sl']:
            position['tr_sl'] = new_long_sl
      # Check to see if it is time to close a short position
      elif position['direction'] == 'SELL':
        if tick['OFR'] > position['tr_sl']:
          close_pos = {
            'dealId': position['deal_id'],
            'direction': 'BUY',
            'orderType': 'MARKET',
            'size': position['size']
          }
          self.trader.closePosition(close_pos)
        # if short position not to close, update sl if necessary
        else:
          new_short_sl = tick['OFR'] + position['tr_sl_dist']
          if new_short_sl > position['tr_sl']:
            position['tr_sl'] = new_short_sl
    #reporter.setCandles(symbol, self.candles)
    #reporter.setTrades(symbol, trader.trades[symbol], trader.balance, trader.num_trades, trader.num_wins, trader.total_r)

  def onAccountUpdate(self, data):
    vals = data['values']
    print(f"ACCOUNT UPDATE: {vals}")
    log_file(f"ACCOUNT UPDATE: {vals}")
    self.available_cash = vals['AVAILABLE_CASH']
    self.profit = vals['PNL']
    if not self.reporter_started:
      self.reporter = Reporter([self.symbol], self.plotter, init_balance=self.available_cash)
      self.reporter_started = True
