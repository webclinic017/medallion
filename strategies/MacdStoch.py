import logging
from datetime import datetime
import pandas_ta as ta
#from ta.volatility import AverageTrueRange, DonchianChannel

class MacdStoch():
  """
    MACD - STOCHASTIC STRATEGY BY MAYANK RASU
    Opening long positions only
    MACD crossover provides entry signals; stochastic to ensure that we avoid whipsaws
    Buy signal: MACD greater than signal; Stochastic greater than 30; Stochastic value increasing
    Trailing SL: Revised after each period, calculated as (period's closing price - 60 period ATR)
    -  Uses 25 most traded stocks on nasdaq - 15 min candles - run on 1 year's data
  """
  def __init__(self, config, trader, plotter, symbols):
    # self.tpR = config['tpR']

    self.orders = []
    self.positions = []
    self.trades = []
    self.trader = trader
    self.amtPerTrade = config['pos_size'] #trader.balance * config['pos_prop']
    self.plotter = plotter
    self.sl = 0
    self.pos_ids = {}

    for symbol in symbols:
      # plotter.createLine(symbol, 'Donchian Low', 'yellow')
      #plotter.createPointsGroup(symbol, 'Stoch30', 'yellow', 'cross')
      #plotter.createPointsGroup(symbol, 'Macd', 'orange', 'cross')
      plotter.createPointsGroup(symbol, 'SL', 'orange', 'cross')
      self.pos_ids[symbol] = None

  def on_candle(self, symbol, candles, db):
    candle = candles.iloc[-1]
    hours = int(datetime.strftime(candle.date, '%H'))
    mins = int(datetime.strftime(candle.date, '%M'))
    macdDf = ta.macd(close=candles['close'], fast=12, slow=21, signal=9)
    stochDf = ta.stoch(high=candles['high'], low=candles['low'], close=candles['close'])
    macd = macdDf.iloc[-1]['MACD_12_21_9']
    macd_signal = macdDf.iloc[-1]['MACDs_12_21_9']
    stoch = stochDf.iloc[-1]['STOCHk_14_3_3']
    prev_stoch = stochDf.iloc[-2]['STOCHk_14_3_3']
    #print(candle.date, 'macd>signal:', macd > macd_signal, 'stoch>30:', stoch>30, 'stoch>prev_stoch:', stoch > prev_stoch)
    buy_signal = bool(macd > macd_signal and stoch > 30 and stoch > prev_stoch)
    #donchian_low = ta.donchian(candles["high"], candles["low"], 20, 20).iloc[-1]["DCL_20_20"]
    #self.plotter.addPtToLine(symbol, 'Donchian Low', candle.date, donchian_low)
    #self.check_positions(candle, candles)

    def getSl(candle, candles):
      atr60 = ta.atr(candles["high"], candles["low"], candles["close"], length=60, mamode="ema").iloc[-1]
      return candle['low'] - atr60
    
    if buy_signal and not self.trader.position_open_symbol(symbol):
      self.sl = getSl(candle, candles)
      self.plotter.addPtToPointsGroup(symbol, 'SL', candle['date'], self.sl)
      self.pos_ids[symbol] = self.trader.openPosition(symbol, 'buy', 1, candle['close'], candle['date'], self.sl)
    elif self.trader.position_open_symbol(symbol):
      updated_sl = getSl(candle, candles)
      if updated_sl > self.sl:
        self.sl = updated_sl
        self.plotter.addPtToPointsGroup(symbol, 'SL', candle['date'], self.sl)
      if candle['low'] < self.sl:
        self.trader.closePosition(self.pos_ids[symbol], self.sl, candle['date'], 'Hit stop loss')


  def on_end(self, symbol, candles):
    candle = candles.iloc[-1]
    self.trader.closeAllPositions(candle, 'End of Strategy')
