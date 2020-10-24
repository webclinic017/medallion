#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:50:34 2020

@author: knbo
"""

def macd(df, maFast=12, maSlow=26, maSignal=9, maType='EMA'):
    macdDf = df.copy()
    if maType == 'SMA':
        macdDf['MA_Fast'] = macdDf['close'].rolling(maFast).mean()
        macdDf['MA_Slow'] = macdDf['close'].rolling(maSlow).mean()
    elif maType == 'EMA':
        macdDf['MA_Fast'] = macdDf['close'].ewm(span=maFast, min_periods=maFast).mean()
        macdDf['MA_Slow'] = macdDf['close'].ewm(span=maSlow, min_periods=maSlow).mean()
    macdDf['MACD'] = macdDf['MA_Fast'] - macdDf['MA_Slow']
    if maType == 'SMA':
        macdDf['Signal'] = macdDf['MACD'].rolling(maSignal).mean()
    elif maType == 'EMA':
        macdDf['Signal'] = macdDf['MACD'].ewm(span=maSignal, min_periods=maSignal).mean()
    return macdDf

def bollingerBands(df, period=20, std=2.0, maType='EMA'):
    bb = df.copy()
    if maType == 'EMA':
        bb['MA'] = bb['close'].ewm(span=period, min_periods=period).mean()
    elif maType == 'SMA':
        bb['MA'] == bb['close'].rolling(period).mean()
    bb['upperBB'] = bb['MA'] + 2 * bb['close'].rolling(period).std(ddof=0)
    bb['lowerBB'] = bb['MA'] - 2 * bb['close'].rolling(period).std(ddof=0)
    bb['widthBB'] = bb['upperBB'] - bb['lowerBB']
    bb.dropna(inplace=True)
    return bb

def atr(df, period=9, maType='EMA'):
    atr = df.copy()
    atr['High-Low'] = abs(df['high'] - df['low'])
    atr['High-PreviousClose'] = abs(df['high'] - df['close'].shift(1))
    atr['Low-PreviousClose'] = abs(df['low'] - df['close'].shift(1))
    atr['TrueRange'] = atr[['High-Low', 'High-PreviousClose', 'Low-PreviousClose']].max(axis=1, skipna=False)
    if maType == 'EMA':
        atr['ATR'] = atr['TrueRange'].ewm(span=period, min_periods=period).mean()
    elif maType == 'SMA':
        atr['ATR'] = atr['TrueRange'].rolling(period).mean()
    return atr

def rsi(df, period=20):
    rsi = df.copy()
    rsi['delta'] = rsi['close'] - rsi['close'].shift(1)
    rsi['gain'] = np.where(rsi['delta']>=0, rsi['delta'], 0)
    rsi['loss'] = np.where(rsi['delta']<0, abs(rsi['delta']), 0)
    avg_gain = []
    avg_loss = []
    gain = rsi['gain'].tolist()
    loss = rsi['loss'].tolist()
    for i in range(len(df)):
        if i < period:
            avg_gain.append(np.NaN)
            avg_loss.append(np.NaN)
        elif i == period:
            avg_gain.append(rsi['gain'].rolling(period).mean()[period])
            avg_loss.append(rsi['loss'].rolling(period).mean()[period])
        elif i > period:
            avg_gain.append(((period - 1) * avg_gain[i - 1] + gain[i])/period)
            avg_loss.append(((period - 1) * avg_loss[i - 1] + loss[i])/period)
    rsi['avg_gain'] = np.array(avg_gain)
    rsi['avg_loss'] = np.array(avg_loss)
    rsi['RS'] = rsi['avg_gain']/rsi['avg_loss']
    rsi['RSI'] = 100 - (100/(1+rsi['RS']))
    return rsi

# ADX (Average Directional Index): measures strength of trend from 0 to 100
# Ranges: 0-25: absent or weak; 25-50: strong; 50-75: very strong; 75-100: extremely strong trend
# It is non directional, only indicates the strength
# The calculation involves comparing successive highs and successive lows, and a smoothed average of these
def adx(df, period=20):
    adx = df.copy()
    adx['High-Low'] = abs(adx['high'] - adx['low'])
    adx['High-PreviousClose'] = abs(adx['high'] - adx['close'].shift(1))
    adx['Low-PreviousClose'] = abs(adx['low'] - adx['close'].shift(1))
    adx['TrueRange'] = adx[['High-Low', 'High-PreviousClose', 'Low-PreviousClose']].max(axis=1, skipna=False)
    adx['+DM']=np.where((adx['high']-adx['high'].shift(1))>(adx['low'].shift(1)-adx['low']),adx['high']-adx['high'].shift(1),0)
    adx['+DM']=np.where(adx['+DM']<0,0,adx['+DM'])
    adx['-DM']=np.where((adx['low'].shift(1)-adx['low'])>(adx['high']-adx['high'].shift(1)),adx['low'].shift(1)-adx['low'],0)
    adx['-DM']=np.where(adx['-DM']<0,0,adx['-DM'])

    adx["+DMMA"]=adx['+DM'].ewm(span=period,min_periods=period).mean()
    adx["-DMMA"]=adx['-DM'].ewm(span=period,min_periods=period).mean()
    adx["TRMA"]=adx['TrueRange'].ewm(span=period,min_periods=period).mean()

    adx["+DI"]=100*(adx["+DMMA"]/adx["TRMA"])
    adx["-DI"]=100*(adx["-DMMA"]/adx["TRMA"])
    adx["DX"]=100*(abs(adx["+DI"]-adx["-DI"])/(adx["+DI"]+adx["-DI"]))
    
    adx["ADX"]=adx["DX"].ewm(span=period,min_periods=period).mean()

    return adx

# Stochastic Oscillator
# Measures the speed or momentum of price change
# Calculation: ((Close - Lowest low)/(Highest High - Lowest Low)) * 100
# Values range from 0 - 100. Close to 100: present price is close to highest price over the look back period
#       Close to 0: present price is close to lowest price over the look back period
# Above 80: overbought; below 20: oversold
def stochasticOsc(df, period=14, maPeriodD=3):
    so = df.copy()
    so['Close-Low'] = so['close'] - so['low'].rolling(period).min()
    so['High-Low'] = so['high'].rolling(period).max() - so['low'].rolling(maPeriodD).min()
    so['%K'] = so['Close-Low'] / so['High-Low'] * 100
    so['%D'] = so['%K'].ewm(span=maPeriodD, min_periods=maPeriodD).mean()
    return so

# Compounded Annual Growth Rate (CAGR)
# Formula: (end value/beginning value)**(1/years)
def cagr(df, dataTimeSpanDays):
    cagr = df.copy()
    cagr['PctChange'] = cagr['close'].pct_change()
    cagr['CompReturn'] = (1 + cagr['PctChange']).cumprod()
    numYears = dataTimeSpanDays/365
    return (cagr['CompReturn'].to_list()[-1]**(1/numYears)) - 1

def volatility(df, numYears=1):
    tradingDaysInYear = 252
    vol = df.copy()
    vol['PctChange'] = vol['close'].pct_change()
    volVal = vol['PctChange'].std() * np.sqrt(tradingDaysInYear * numYears)
    return volVal

def sharpe(df, riskFreeRate, dataTimeSpanInDays):
    sh = df.copy()
    return (cagr(df, dataTimeSpanInDays) - riskFreeRate) / volatility(sh)

def maxDrawDown(df):
    dd = df.copy()
    dd['PctChange'] = dd['close'].pct_change()
    dd['CumulativeRet'] = (1 + dd['PctChange']).cumprod()
    dd['MaxCumRet'] = dd['CumulativeRet'].cummax()
    dd['DrawDown'] = dd['MaxCumRet'] - dd['CumulativeRet']
    dd['DrawDownPct'] = dd['DrawDown'] / dd['MaxCumRet']
    return dd['DrawDownPct'].max()