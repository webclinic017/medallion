#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 11:10:44 2020

@author: knbo
"""

#%%
import pandas as pd
import asyncio
from copy import deepcopy
from IbTrader import IbTrader
import indicators as ind
import numpy as np

ib = IbTrader()
ib.start()

#ib.getHistoricalData('FB', '1 M', '15 mins')
#%%

tickers = ["FB","AMZN","INTC","MSFT","AAPL","GOOG","CSCO","CMCSA","ADBE","NVDA",
           "NFLX","PYPL","AMGN","AVGO","TXN","CHTR","QCOM","GILD","FISV","BKNG",
           "INTU","ADP","CME","TMUS","MU"]
daysInTestRange = 365

#%%
def loadHistoricalData():
    for ticker in tickers:
        ib.getHistoricalData(ticker, '1 Y', '15 mins')
loadHistoricalData()

#%%
#tickerData = {}
reqs = []
for ticker in tickers:
    reqs.append(ib.getHistoricalData(ticker, '1 M', '15 mins'))

tickerData = await asyncio.gather(*reqs)
print(f'GOT TICKER DATA')

#%%
signal = {}
pct_change = {}
trade_count = {}
def addIndicators(s):
    stks = deepcopy(s)
    #stks['trade_count'] = 0
    #stks['signal'] = ''
    #stks['DailyReturn'] = ''
    #stks['DailyReturn'] = stks['DailyReturn'].apply(list)
    for ticker, stk in stks.items():
        print('Adding indicators for', ticker)
        df = stk['data']
        df['Stoch'] = ind.stochasticOsc(df)['%K']
        df['MACD'] = ind.macd(df)['MACD']
        df['MacdSignal'] = ind.macd(df)['Signal']
        df['ATR'] = ind.atr(df, period=60)['ATR']
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        signal[ticker] = ''
        pct_change[ticker] = [0]
        trade_count[ticker] = 0
    return stks
stks = addIndicators(ib.stks)
#%%
# =============================================================================
# ohlc_dict = deepcopy(historicalData)
# tickers_signal = {}
# tickers_ret = {}
# trade_count = {}
# for ticker in tickers:
#     print("Calculating MACD & Stochastics for ",ticker)
#     ohlc_dict[ticker]["stoch"] = stochOscltr(ohlc_dict[ticker])
#     ohlc_dict[ticker]["macd"] = MACD(ohlc_dict[ticker])["MACD"]
#     ohlc_dict[ticker]["signal"] = MACD(ohlc_dict[ticker])["Signal"]
#     ohlc_dict[ticker]["atr"] = atr(ohlc_dict[ticker],60)
#     ohlc_dict[ticker].dropna(inplace=True)
#     trade_count[ticker] = 0
#     tickers_signal[ticker] = ""
#     tickers_ret[ticker] = [0]
# =============================================================================
#%%
def addSignalsAndReturn(stks):
    s = deepcopy(stks)
    for ticker, stk in s.items():
        print('Adding signals to', ticker)
        df = stk['data']
        for idx, candle in df.iterrows():
            if idx > 0:
                if signal[ticker] == '':
                    pct_change[ticker].append(0)
                    if candle['MACD'] > candle['MacdSignal'] and \
                        candle['Stoch'] > 30 and \
                        candle['Stoch'] > df.iloc[idx - 1]['Stoch']:
                            signal[ticker]= 'Buy'
                            trade_count[ticker] += 1
                elif signal[ticker]== 'Buy':
                    if candle['low']< df.iloc[idx - 1]['close'] - df.iloc[idx - 1]['ATR']:
                        signal[ticker] = ''
                        trade_count[ticker] += 1
                        pct_change[ticker].append(((df.iloc[idx - 1]['close'] - df.iloc[idx - 1]['ATR']) / df.iloc[idx - 1]['close']) - 1)
                    else:
                        pct_change[ticker].append((candle['close'] / df.iloc[idx - 1]['close']) - 1)
        stk['PctChange'] = np.array(pct_change[ticker]) 
    return s
stks = addSignalsAndReturn(stks)
#%%
#Identifying Signals and calculating daily return (Stop Loss factored in)
# =============================================================================
# for ticker in tickers:
#     print("Calculating daily returns for ",ticker)
#     for i in range(1,len(ohlc_dict[ticker])):
#         if tickers_signal[ticker] == "":
#             tickers_ret[ticker].append(0)
#             if ohlc_dict[ticker]["macd"][i]> ohlc_dict[ticker]["signal"][i] and \
#                ohlc_dict[ticker]["stoch"][i]> 30 and \
#                ohlc_dict[ticker]["stoch"][i] > ohlc_dict[ticker]["stoch"][i-1]:
#                    tickers_signal[ticker] = "Buy"
#                    trade_count[ticker]+=1
#                      
#         elif tickers_signal[ticker] == "Buy":
#             if ohlc_dict[ticker]["Low"][i]<ohlc_dict[ticker]["Close"][i-1] - ohlc_dict[ticker]["atr"][i-1]:
#                 tickers_signal[ticker] = ""
#                 trade_count[ticker]+=1
#                 tickers_ret[ticker].append(((ohlc_dict[ticker]["Close"][i-1] - ohlc_dict[ticker]["atr"][i-1])/ohlc_dict[ticker]["Close"][i-1])-1)
#             else:
#                 tickers_ret[ticker].append((ohlc_dict[ticker]["Close"][i]/ohlc_dict[ticker]["Close"][i-1])-1)
#                 
#                 
#     ohlc_dict[ticker]["ret"] = np.array(tickers_ret[ticker])
# =============================================================================
    
#%%
# calculating overall strategy's KPIs

strategy_df = pd.DataFrame()
for ticker in tickers:
    print('Building strategy_df:', ticker)
    strategy_df[ticker] = stks[ticker]['PctChange']
strategy_df['PctChange'] = strategy_df.mean(axis=1)
#strategy_df = pd.DataFrame()
#for ticker in tickers:
#    strategy_df[ticker] = ohlc_dict[ticker]["ret"]
#strategy_df["ret"] = strategy_df.mean(axis=1)

"""
# creating dataframe with daily returns
strategy_df.index = pd.to_datetime(strategy_df.index)
strategy_df.index = strategy_df.index.tz_localize('Asia/Kolkata').tz_convert('America/Indiana/Petersburg')    
daily_ret_df = strategy_df["ret"].resample("D").sum(min_count=1).dropna()

# plotting daily returns
daily_ret_df.plot(kind="bar") 
"""
#%%
cagr = ind.cagr(strategy_df, daysInTestRange)
sharpe = ind.sharpe(strategy_df,0.025, daysInTestRange)
maxDrawDown = ind.maxDrawDown(strategy_df)
print(f'OVERALL KPIs. CAGR: {cagr}; SHARPE RATIO: {sharpe}; MAX DRAWDOWN: {maxDrawDown};')

#%%
# vizualization of strategy return
(1+strategy_df["PctChange"]).cumprod().plot()

#%%
#calculating individual stock's KPIs
cagr = {}
sharpe_ratios = {}
max_drawdown = {}
for ticker in tickers:
    print("calculating KPIs for ",ticker)      
    cagr[ticker] =  ind.cagr(stks[ticker], daysInTestRange)
    sharpe_ratios[ticker] =  ind.sharpe(stks[ticker],0.025, daysInTestRange)
    max_drawdown[ticker] =  ind.maxDrawDown(stks[ticker])
    
KPI_df = pd.DataFrame([cagr,sharpe_ratios,max_drawdown],index=["Return","Sharpe Ratio","Max Drawdown"])      
KPI_df.T