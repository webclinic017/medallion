import numpy as np
import logging

class PriceTargetStrategy():
  def __init__(self, config, trader, plotter, symbols):
    self.tpR = config['tpR']
    
    self.orders = []
    self.positions = []
    self.trades = []
    self.trader = trader
    self.amtPerTrade = 1000 #trader.balance * config['pos_prop']
    self.plotter = plotter
    self.plotter.createLine('PriceTarget', 'Price Target', 'green')

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
