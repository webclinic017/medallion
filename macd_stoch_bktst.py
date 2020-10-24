#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 11:10:44 2020

@author: knbo
"""

#%%
import pandas as pd
from copy import deepcopy
from IbTrader import IbTrader
import indicators as ind
import numpy as np

tickers = ["FB","AMZN","INTC","MSFT","AAPL","GOOG","CSCO","CMCSA","ADBE","NVDA",
           "NFLX","PYPL","AMGN","AVGO","TXN","CHTR","QCOM","GILD","FISV","BKNG",
           "INTU","ADP","CME","TMUS","MU"]

ib = IbTrader()
ib.start()

def loadHistoricalData():
    for ticker in tickers:
        ib.getHistoricalData(ticker, '1 Y', '15 mins')
        
#%%
signal = {}
pct_change = {}
trade_count = {}
def addIndicators(df):
    stks = df.copy()
    #stks['trade_count'] = 0
    #stks['signal'] = ''
    #stks['DailyReturn'] = ''
    #stks['DailyReturn'] = stks['DailyReturn'].apply(list)
    for index, stk in stks.iterrows():
        print('Adding indicators for', stk.ticker)
        stk.ticker_data['Stoch'] = ind.stochasticOsc(stk.ticker_data)['%K']
        stk.ticker_data['MACD'] = ind.macd(stk.ticker_data)['MACD']
        stk.ticker_data['MacdSignal'] = ind.macd(stk.ticker_data)['Signal']
        stk.ticker_data['ATR'] = ind.atr(stk.ticker_data, period=60)['ATR']
        stk.ticker_data.dropna(inplace=True)
        stk.ticker_data.reset_index(drop=True, inplace=True)
        signal[stk.ticker] = ''
        pct_change[stk.ticker] = [0]
        trade_count[stk.ticker] = 0
    return stks

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
def addSignalsAndReturn(df):
    sigs = df.copy()
    for index, stk in sigs.iterrows():
        print('Adding signals to', stk.ticker)
        for idx, candle in stk['ticker_data'].iterrows():
            if idx > 0:
                if signal[stk.ticker] == '':
                    if candle['MACD'] > candle['MacdSignal'] and \
                        candle['Stoch'] > 30 and \
                        candle['Stoch'] > stk['ticker_data'].iloc[idx - 1]['Stoch']:
                            signal[stk.ticker]= 'Buy'
                            trade_count[stk.ticker] += 1
                elif signal[stk.ticker]== 'Buy':
                    if candle['low']< stk['ticker_data'].iloc[idx - 1]['close'] - stk['ticker_data'].iloc[idx - 1]['ATR']:
                        signal[stk.ticker] = ''
                        trade_count[stk.ticker] += 1
                        pct_change[stk.ticker].append(((stk['ticker_data'].iloc[idx - 1]['close'] - stk['ticker_data'].iloc[idx - 1]['ATR']) / stk['ticker_data'].iloc[idx - 1]['close']) - 1)
                    else:
                        pct_change[stk.ticker].append((candle['close'] / stk['ticker_data'].iloc[idx - 1]['close']) - 1)
        stk['PctChange'] = np.array(pct_change[stk.ticker]) 
    return sigs
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
    strategy_df[ticker] = 
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

CAGR(strategy_df)
sharpe(strategy_df,0.025)
max_dd(strategy_df)

#%%
# vizualization of strategy return
(1+strategy_df["ret"]).cumprod().plot()

#%%
#calculating individual stock's KPIs
cagr = {}
sharpe_ratios = {}
max_drawdown = {}
for ticker in tickers:
    print("calculating KPIs for ",ticker)      
    cagr[ticker] =  CAGR(ohlc_dict[ticker])
    sharpe_ratios[ticker] =  sharpe(ohlc_dict[ticker],0.025)
    max_drawdown[ticker] =  max_dd(ohlc_dict[ticker])
    
KPI_df = pd.DataFrame([cagr,sharpe_ratios,max_drawdown],index=["Return","Sharpe Ratio","Max Drawdown"])      
KPI_df.T