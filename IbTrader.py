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

class IbTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.barCount = 0
        self.nasdaqStks = pd.DataFrame(columns=['ticker', 'ticker_data'])
    
    def start(self):
        print('\nCONNECTING...')
        self.connect('127.0.0.1', 7497, clientId=1)

        self.appFinished = threading.Event()
        threading.Thread(target=self.run_app).start()
        time.sleep(5)

        threading.Thread(target=self.stop_app).start()
        print('\nCONNECTED AND READY!\n')

    def stop(self):
        time.sleep(10)
        self.appFinished.set()

    def run_app(self):
        self.run()

    def stop_app(self):
        self.appFinished.wait()
        if self.appFinished.is_set():
            self.disconnect()
            print('\nSTOPPED APP\n')
        
    
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
        
    def getNasdaqContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.currency = 'USD'
        contract.exchange = 'ISLAND' # alias for NASDAQ
        return contract

    def getHistoricalData(self, ticker, duration, candle_size):
        reqId = len(self.nasdaqStks)
        contract = self.getNasdaqContract(ticker)
        print('\nREQUESTING HISTORICAL DATA', reqId, contract, '\n')
        # Initialize ticker in the dataframe
        self.nasdaqStks = app.nasdaqStks.append({'ticker': ticker, 'ticker_data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}, ignore_index=True)
        self.reqHistoricalData(
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
app = IbTrader()
app.start()
#%%
#app.reqContractDetails(111, contract)
tickers=['FB', 'AMZN']
for ticker in tickers:
    app.getHistoricalData(ticker, '1 W', '1 day')

app.stop()