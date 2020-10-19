#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 20:51:27 2020

@author: knbo
"""

#%%
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd

class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.barCount = 0
        self.nasdaqStks = pd.DataFrame(columns=['ticker', 'ticker_data'])
    
    def error(self, reqId, errorCode, errorString):
        print("Error {} {} {}".format(reqId, errorCode, errorString))
        
    def contractDetails(self, reqId, contractDetails):
        print(f'reqId: {reqId}; contract: {contractDetails}')
        
    def historicalData(self, reqId, bar):
        self.barCount += 1
        self.nasdaqStks.iloc[reqId]['ticker_data'] = self.nasdaqStks.iloc[reqId]['ticker_data'].append({'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume}, ignore_index=True)
        #print('got data', bar.date)


    def historicalDataEnd(self, reqId, start, end):
        print('\nEND OF HISTORIAL DATA FOR', app.nasdaqStks.iloc[reqId].ticker, 'START:', start, 'END:', end)
        print(app.nasdaqStks.iloc[reqId].ticker_data)

def run_app():
    print('\nSTARTED APP\n')
    app.run()

def stop_app():
    appFinished.wait()
    if appFinished.is_set():
        app.disconnect()
        print('\nSTOPPED APP\n')
        
def getNasdaqContract(symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.currency = 'USD'
    contract.exchange = 'ISLAND' # alias for NASDAQ
    return contract

def getHistoricalData(ticker, duration, candle_size):
    reqId = len(app.nasdaqStks)
    contract = getNasdaqContract(ticker)
    print('\nREQUESTING HISTORICAL DATA', reqId, contract, '\n')
    # Initialize ticker in the dataframe
    app.nasdaqStks = app.nasdaqStks.append({'ticker': ticker, 'ticker_data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}, ignore_index=True)
    app.reqHistoricalData(
        reqId=reqId,
        contract=contract,
        endDateTime='', # empty string = current time
        durationStr=duration,
        barSizeSetting=candle_size,
        whatToShow='TRADES',
        useRTH=1,
        formatDate=1,
        keepUpToDate=0,
        chartOptions=[])

#%%
""" def initStks(tickers):
    for ticker in tickers:
        app.nasdaqStks = app.nasdaqStks.append({'ticker': ticker, 'ticker_data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}, ignore_index=True)

def getTickerIndex(ticker):
    return app.nasdaqStks.loc[app.nasdaqStks['ticker'] == ticker].index[0]
 """
#%%
app = TradingApp()
app.connect('127.0.0.1', 7497, clientId=1)

appFinished = threading.Event()
threading.Thread(target=run_app).start()
time.sleep(5)

threading.Thread(target=stop_app).start()

#%%
#app.reqContractDetails(111, contract)
tickers=['FB', 'AMZN']
for ticker in tickers:
    getHistoricalData(ticker, '1 W', '1 day')

time.sleep(30)
appFinished.set()