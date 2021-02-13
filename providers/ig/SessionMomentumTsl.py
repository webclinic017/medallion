import logging
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
import json
#from ta.volatility import AverageTrueRange, DonchianChannel

class SessionMomentumTsl():
  """ Session Momentum Strategy with Trailing Stop Loss """
  def __init__(self, strat_opts, trader, plotter, symbol):
    self.plotter = plotter
    self.trader = trader
    self.symbol = symbol
    self.num_warm_up_candles = strat_opts['num_warm_up_candles']

    #  Old variables
    self.orders = []
    self.trades = []
    self.state = 'False'
    self.isPosOpen = False
    self.dayAtr = None

    self.pos_size = strat_opts['pos_size'] #trader.balance * config['pos_prop']
    self.traded_today = False
    
    self.range_max = None
    self.range_min = None
    self.available_cash = 0
    self.profit = 0
    self.candles = None

    self.plotter.createLine(symbol, 'Donchian High', 'yellow')
    self.plotter.createLine(symbol, 'Donchian Low', 'yellow')

  def onPositionOpen(self, position):
    self.traded_today = True
  
  def onPositionClosed(self, position):
    pass

  def onCandle(self, candle):
    # Get the candle's hour and minutes
    candle_time = datetime.fromtimestamp(int(candle['UTM'])/1000)
    hour = int(candle_time.strftime('%H'))
    mins = int(candle_time.strftime('%M'))

    # Save warm up candles to calculate the Donchian Channel
    if self.candles == None:
      self.candles = pd.DataFrame([candle])
    else:
      self.candles.append(candle, ignore_index=True)
      if len(self.candles.index) > self.num_warm_up_candles:
        self.candles = self.candles[1:]

    # Calculate overnight range
    if hour == 22 and mins == 0:
      self.range_max = candle['OFR_HIGH']
      self.range_min = candle['BID_LOW']
      self.overnightDatetimeStart = candle_time
    elif hour == 22 and mins > 0 or hour > 22 or hour < 8:
      if candle['OFR_HIGH'] > self.range_max:
        self.range_max = candle['OFR_HIGH']
      if candle['BID_LOW'] < self.range_min:
        self.range_min  = candle['BID_LOW']
    
    # It's trading time
    if len(self.candles.index) == self.num_warm_up_candles:
      # Trading time initialisation work
      if hour == 8 and mins == 0:
        self.tradedToday = False
        # Calculate ATR for the day  
        self.day_ATR = self.getDayAtr(period=14)
        # Draw the overnight range rectangle on the chart
        print('Trading Open:', candle_time)
        rect_name = f'Overnight Range {candle_time.strftime("%Y-%m-%d")}'
        self.plotter.createLine(self.symbol, rect_name, 'grey')
        self.plotter.addPtToLine(self.symbol, rect_name, self.overnightDatetimeStart, self.range_min)
        self.plotter.addPtToLine(self.symbol, rect_name, self.overnightDatetimeStart, self.range_max)
        self.plotter.addPtToLine(self.symbol, rect_name, candle_time, self.range_max)
        self.plotter.addPtToLine(self.symbol, rect_name, candle_time, self.range_min)
        self.plotter.addPtToLine(self.symbol, rect_name, self.overnightDatetimeStart, self.range_min)

      # The actual trading time
      if hour >= 8 and hour < 22 and len(self.candles.index) > self.num_warm_up_candles:
        # Calculate the Donchian Channel
        donchian = ta.donchian(self.candles["OFR_HIGH"], self.candles["BID_LOW"], 20, 20).iloc[-1]
        don_high = donchian['DCH_20_20']
        don_low = donchian['DCL_20_20']
        self.plotter.addPtToLine(self.symbol, 'Donchian High', candle_time, don_high)
        self.plotter.addPtToLine(self.symbol, 'Donchian Low', candle_time, don_low)
        # Get previous candle
        prev_candle = self.candles.iloc[-2]
        # Prepare for opening a position
        position = {
          'currencyCode': 'GBP',
          'epic': self.symbol,
          'orderType': 'MARKET',
          'size': self.pos_size,
          'trailingStop': False
        }
        # Check for a long position
        if prev_candle['OFR_CLOSE'] > self.range_max and not self.traded_today:
          sl = don_low
          sl_distance = candle['OFR_OPEN'] - sl
          actual_range = (sl_distance + candle['OFR_OPEN'] - self.range_min)
          valid_session = self.day_ATR > actual_range
          if valid_session:
            position['direction'] = 'BUY'
            position['deal_ref'] = self.trader.openPosition(position)
            position['tr_sl'] = sl
            position['tr_sl_dist'] = sl_distance
            self.trader.pos_to_confirm.append(position)

        # Check for a short position
        if prev_candle['BID_CLOSE'] < self.range_min and not self.traded_today:
          sl = don_low
          sl_distance = candle['BID_OPEN'] - sl
          actual_range = (sl_distance + candle['BID_OPEN'] - self.range_max)
          valid_session = self.day_ATR > actual_range
          if valid_session:
            position['direction'] = 'SELL'
            position['deal_ref'] = self.trader.openPosition(position)
            position['tr_sl'] = sl
            position['tr_sl_dist'] = sl_distance
            self.trader.pos_to_confirm.append(position)

  def getDayAtr(self, period):
    day_candles = self.trader.getPriceHistory(epic=self.symbol, resolution='DAY', max=period)
    atr_dict = map(lambda c: {
      'high':  c['highPrice']['ask'],
      'low': c['lowPrice']['bid'],
      'close': c['closePrice']['bid']
    }, day_candles)
    atr_df = pd.DataFrame(atr_dict)
    return ta.atr(atr_df['high'], atr_df['low'], atr_df['close'], length=period, mamode="ema").iloc[-1]

  def getOvernightRange(self):
    def getRange(prices):
      max = prices[0]['highPrice']['ask']
      min = prices[0]['lowPrice']['bid']
      for i in range(1, len(prices)):
          if prices[i]['highPrice']['ask'] > max:
              max = prices[i]['highPrice']['ask']
          if prices[i]['lowPrice']['bid'] < min:
              min = prices[i]['lowPrice']['bid']
      return {'min': min, 'max': max}
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    year = int(today.strftime('%Y'))
    today_month = int(today.strftime('%m'))
    yesterday_month = int(yesterday.strftime('%m'))
    today_date = int(today.strftime('%d'))
    yesterday_date = int(yesterday.strftime('%d'))
    from_dt = datetime(year, yesterday_month, yesterday_date, 22)
    to_dt = datetime(year, today_month, today_date, 8)
    hist_prices = self.trader.getPriceHistory(epic=self.symbol, from_dt=from_dt, to_dt=to_dt)
    return getRange(hist_prices)

  def on_end(self, symbol, candles):
    candle = candles.iloc[-1]
    self.trader.closeAllPositions(candle)
