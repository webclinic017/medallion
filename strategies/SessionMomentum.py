import logging
from datetime import datetime
import pandas_ta as ta
#from ta.volatility import AverageTrueRange, DonchianChannel

class SessionMomentum():
  def __init__(self, config, trader, plotter, symbols):
    self.tpR = config['tpR']

    self.orders = []
    self.positions = []
    self.trades = []
    self.trader = trader
    self.amtPerTrade = config['pos_size'] #trader.balance * config['pos_prop']
    self.plotter = plotter
    self.overnightDateStart = None
    self.rangeMax = None
    self.rangeMin = None
    self.state = 'BEFORE_RANGE'
    self.tradedToday = False
    self.isPosOpen = False
    self.dayAtr = None
    for symbol in symbols:
      plotter.createLine(symbol, 'Donchian Low', 'yellow')

  def checkOrders(self, candle, candles):
    for order in self.trader.orders:
      if order['placed'] == candle.date:
        continue
      if order['status'] == 'active':
        if candle['high'] > order['tp_price']:
          self.trader.closePosition(order, price_sold=order['tp_price'], date=candle.date)
          self.isPosOpen = False
          #print('Close pos TP')
          self.state = 'TRADING_OPEN'
        elif candle['low'] < order['sl_price']:
          self.trader.closePosition(order, price_sold=order['sl_price'], date=candle.date)
          self.isPosOpen = False
          #print('Close pos SL')
          self.state = 'TRADING_OPEN'

  def on_candle(self, symbol, candles, db):
    candle = candles.iloc[-1]
    prevCandle = candles.iloc[-2]
    hours = int(datetime.strftime(candle.date, '%H'))
    mins = int(datetime.strftime(candle.date, '%M'))
    donchian_low = ta.donchian(candles["high"], candles["low"], 20, 20).iloc[-1]["DCL_20_20"]
    self.plotter.addPtToLine(symbol, 'Donchian Low', candle.date, donchian_low)
    self.checkOrders(candle, candles)
    if self.state == 'BEFORE_RANGE':
      if hours > 22:
        self.state = 'RANGE_CALC'
        self.rangeMax = candle.high
        self.rangeMin = candle.low
        self.overnightDateStart = candle.date
    if self.state == 'RANGE_CALC':
      if candle.low < self.rangeMin:
        self.rangeMin = candle.low
      elif candle.high > self.rangeMax:
        self.rangeMax = candle.high
      if hours == 7 and mins == 55:
        self.trader.closeAllPositions(candle)
        self.isPosOpen = False
      if hours == 8 and mins == 0:
        self.tradedToday = False
        self.state = 'TRADING_OPEN'
        atr_period = 14
        table_name = f'ib_{symbol}_1D'
        dayCandles = db.getLastCandles(table_name, candle.date.to_pydatetime(), atr_period)
        #myAtr = self.atr(candles["high"], candles["low"], candles["close"], 4032).iloc[-1]
        self.dayAtr = ta.atr(dayCandles["high"], dayCandles["low"], dayCandles["close"], length=atr_period, mamode="ema").iloc[-1]
        #avg = AverageTrueRange(high=candles["high"], low=candles["low"], close=candles["close"], window=288)
        #print('Trading Open:', candle.date, 'ATR:', avg.average_true_range().iloc[-1], dayAtr, myAtr)
        print('Trading Open:', candle.date, 'Total R:', self.trader.total_r)
        rect_name = f'Overnight Range {datetime.strftime(candle.date, "%Y-%m-%d")}'
        self.plotter.createLine(symbol, rect_name, 'grey')
        self.plotter.addPtToLine(symbol, rect_name, self.overnightDateStart, self.rangeMin)
        self.plotter.addPtToLine(symbol, rect_name, self.overnightDateStart, self.rangeMax)
        self.plotter.addPtToLine(symbol, rect_name, candle.date, self.rangeMax)
        self.plotter.addPtToLine(symbol, rect_name, candle.date, self.rangeMin)
        self.plotter.addPtToLine(symbol, rect_name, self.overnightDateStart, self.rangeMin)

        #print('\nReady to trade. Max:', self.rangeMax, 'Min:', self.rangeMin, candle.date)
    if self.state == 'TRADING_OPEN':
      #print('prevCandle.close:', prevCandle.close, self.rangeMax, 'Is greater than max:', prevCandle.close > self.rangeMax)
      if prevCandle.close > self.rangeMax and not self.tradedToday and not self.isPosOpen:
        #slLength = (self.rangeMax - self.rangeMin) / 2
        # rAbsolute = slLength + candle.open - self.rangeMax
        #sl = candle.open - slLength # Place sl at half the range height
        sl = donchian_low
        
        slLength = candle.open - sl
        #print('don:', don)
        tp = candle.open + (self.tpR * slLength)
        actual_range = (slLength + candle.open - self.rangeMin)
        valid_session = self.dayAtr > actual_range
        # print('    Valid:', valid_session)
        if valid_session:
          self.trader.placeMktBracketOrder(symbol, 'BUY', self.amtPerTrade, candle.open, tp, sl, candle.date)
          #print('Trade placed.', candle.open, candle.date)
          self.isPosOpen = True
        self.tradedToday = True
      if prevCandle.close < self.rangeMin and not self.tradedToday and not self.isPosOpen:
        self.tradedToday = True
      if hours == 21 and mins == 0:
        self.state = 'BEFORE_RANGE'
        #print('Finished trading for the day')


  def on_end(self, symbol, candles):
    candle = candles.iloc[-1]
    self.trader.closeAllPositions(candle)
