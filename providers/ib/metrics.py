#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 18:35:38 2020

@author: knbo
"""
import pandas as pd
import numpy as np



# Compounded Annual Growth Rate (CAGR)
# Formula: (end value/beginning value)**(1/years)
def cagr(df, dataTimeSpanDays):
    cagr = df.copy()
    #cagr['PctChange'] = cagr['close'].pct_change()
    cagr['CompReturn'] = (1 + cagr['PctChange']).cumprod()
    numYears = dataTimeSpanDays/365
    return (pd.Series(cagr['CompReturn']).to_list()[-1]**(1/numYears)) - 1

def volatility(df, numYears=1):
    tradingDaysInYear = 252
    vol = df.copy()
    #vol['PctChange'] = vol['close'].pct_change()
    volVal = vol['PctChange'].std() * np.sqrt(tradingDaysInYear * numYears)
    return volVal

def sharpe(df, riskFreeRate, dataTimeSpanInDays):
    sh = df.copy()
    return (cagr(sh, dataTimeSpanInDays) - riskFreeRate) / volatility(sh)

def maxDrawDown(df):
    dd = df.copy()
    #dd['PctChange'] = dd['close'].pct_change()
    dd['CumulativeRet'] = (1 + dd['PctChange']).cumprod()
    dd['MaxCumRet'] = pd.Series(dd['CumulativeRet']).cummax()
    dd['DrawDown'] = dd['MaxCumRet'] - dd['CumulativeRet']
    dd['DrawDownPct'] = dd['DrawDown'] / dd['MaxCumRet']
    return dd['DrawDownPct'].max()

def winRate(DF):
    "function to calculate win rate of intraday trading strategy"
    df = DF["PctChange"]
    pos = df[df>1]
    neg = df[df<1]
    return (len(pos)/len(pos+neg))*100

def meanReturnPerTrade(DF):
    df = DF["PctChange"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp!=0].mean()

def meanReturnWinningTrades(DF):
    df = DF["PctChange"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp>0].mean()

def meanReturnLosingTrades(DF):
    df = DF["PctChange"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp<0].mean()

def maxNumConsecutiveLosses(DF):
    df = DF["PctChange"]
    df_temp = df.dropna(axis=0)
    df_temp2 = np.where(df_temp<1,1,0)
    count_consecutive = []
    seek = 0
    for i in range(len(df_temp2)):
        if df_temp2[i] == 0:
            seek = 0
        else:
            seek = seek + 1
            count_consecutive.append(seek)
    return max(count_consecutive)