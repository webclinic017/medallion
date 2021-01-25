import numpy as np
import logging

class BiasBarStrategy():
  def __init__(self, config, trader, plotter, symbols):
    self.tpR = config['tpR']
    self.closeOrderAfter = config['closeOrderAfter']
    self.retracementLevelPct = config['retracementLevelPct']
    self.smaSize = config['smaSize']
    
    self.orders = []
    self.positions = []
    self.trades = []
    self.trader = trader
    self.amtPerTrade = 1000 #trader.balance * config['pos_prop']
    self.plotter = plotter
    for symbol in symbols:
      self.plotter.createPointsGroup(symbol, 'Engulfing', 'blue', 'cross')
  
  def is_engulfing(self, candles):
    # conditions for engulfing
    actual_candle = candles.iloc[-1]
    previous_candle = candles.iloc[-2]
    # 1. Previous candle is red
    prev_is_red = previous_candle['open'] > previous_candle['close']
    # 2. Actual candle is green
    act_is_green = actual_candle['open'] < actual_candle['close']
    # 3. Actual engulfs Previous
    engulfs = actual_candle['open'] < previous_candle['close'] and actual_candle['close'] > previous_candle['open']
    return prev_is_red and act_is_green and engulfs

  def checkOrders(self, candle, candles):
    for order in self.trader.orders:
      if order['placed'] == candle.date:
        continue
      if order['status'] == 'pending':
        #print(f'Pending order:', order)
        if candle.low <= order['lmt_price']: # and self.is_uptrend(candles):
          self.trader.openPosition(order, candle.date)
        elif order['candles_pending'] >= self.closeOrderAfter:
          order['status'] = 'cancelled'
          logging.debug(f'{order["symbol"]} Cancel stale order placed on {order["placed"]}')
        else:
          order['candles_pending'] += 1
      if order['status'] == 'active':
        if candle['high'] > order['tp_price']:
          self.trader.closePosition(order, price_sold=order['tp_price'], date=candle.date)
        if candle['low'] < order['sl_price']:
          self.trader.closePosition(order, price_sold=order['sl_price'], date=candle.date)
    for order in self.trader.orders:
      if order['status'] == 'cancelled':
        self.trader.orders.remove(order)
  
  def is_uptrend(self, candles):
    candles['SMA'] = candles['close'].rolling(self.smaSize).mean()
    candle = candles.iloc[-1]
    return candle.low > candle.SMA


  def on_candle(self, symbol, candles):
    candle = candles.iloc[-1]
    if self.is_engulfing(candles):
      self.plotter.addPtToPointsGroup(symbol, 'Engulfing', candle.date, candle.high)
      retracementLevel = ((candle.high - candle.low) * (100 - self.retracementLevelPct) / 100) + candle.low
      logging.debug(f'{candle.date} {symbol} ENGULFING. H:{candle.high} L:{candle.low} retLev:{retracementLevel}')
      sl = candle.low * 0.9995
      #tpAnotherEngCandleLength = candleDf.loc[index-1, 'close'] - candleDf.loc[index-1, 'open'] + candleDf.loc[index-1, 'close']
      static_r_tp = ((retracementLevel - sl) * self.tpR) + retracementLevel
      logging.debug(f'Place order for when price reaches {retracementLevel}')
      #if self.is_uptrend(candles):
      self.trader.placeLmtBracketOrder(
          symbol=symbol,
          action='BUY',
          quantity=self.amtPerTrade/retracementLevel,
          lmt_price=retracementLevel,
          tp_price=static_r_tp,
          sl_price=sl,
          placed=candle.date)
    self.checkOrders(candle, candles)
  
  def on_end(self, symbol, candles):
    candle = candles.iloc[-1]
    self.trader.closeAllPositions(candle)
