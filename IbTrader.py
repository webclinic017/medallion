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
from ibapi.order import Order
import threading
import time
import pandas as pd
import numpy as np

class IbTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.barCount = 0
        self.stks = pd.DataFrame(columns=['ticker', 'ticker_data'])
        self.ordersPlaced = []
        self.orderDf = pd.DataFrame(columns=[
                'PermId', 'ClientId', 'OrderId', 'Account', 'Symbol', 'SecType', 'Exchange',
                'Action', 'OrderType', 'TotalQty', 'CashQty', 'LmtPrice', 'AuxPrice', 'Status'
            ])
        self.positionDf = pd.DataFrame(columns=[
                'Account', 'Symbol', 'SecType', 'Currency', 'Position', 'Avg cost'
            ])
        self.accountSummaryDf = pd.DataFrame(columns=['ReqId', 'Account', 'Tag', 'Value', 'Currency'])
        self.pnlDf = pd.DataFrame(columns=['ReqId', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL'])
    
    def start(self):
        print('\nCONNECTING...')
        self.connect('127.0.0.1', 7497, clientId=1)

        self.appFinished = threading.Event()
        threading.Thread(target=self.run_app).start()
        time.sleep(5)

        threading.Thread(target=self.stop_app).start()
        print('\nIF YOU HAVEN\'T SEEN ANY ERRORS... CONNECTED AND READY!\n')

    def stop(self):
        self.appFinished.set()

    def run_app(self):
        self.run()

    def stop_app(self):
        self.appFinished.wait()
        if self.appFinished.is_set():
            self.disconnect()
            print('\nDISCONNECTED FROM IB\n')
    
    ##########################
    ### EWrapper callbacks ###
    ##########################
    def error(self, reqId, errorCode, errorString):
        if reqId == -1:
            print(f'{errorCode} {errorString}')
        else:
            print("Error {} {} {}".format(reqId, errorCode, errorString))
        
    def contractDetails(self, reqId, contractDetails):
        print(f'reqId: {reqId}; contract: {contractDetails}')
        
    def historicalData(self, reqId, bar):
        self.barCount += 1
        self.stks.iloc[reqId]['ticker_data'] = self.stks.iloc[reqId]['ticker_data'].append({'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume}, ignore_index=True)
        #print(f'{self.stks.iloc[reqId].ticker} ', end='')

    def historicalDataEnd(self, reqId, start, end):
        print('\nEND OF HISTORIAL DATA FOR', self.stks.iloc[reqId].ticker, 'START:', start, 'END:', end)
        print(self.stks.iloc[reqId].ticker_data)
    
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print('nextValidOrderId:', orderId)
    
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {
                'PermId': order.permId, 'ClientId': 'order.clientId', 'OrderId': orderId,
                'Account': order.account, 'Symbol': contract.symbol, 'SecType': contract.secType,
                'Exchange': contract.exchange, 'Action': order.action, 'OrderType': order.orderType,
                'TotalQty': order.totalQuantity, 'CashQty': order.cashQty,
                'LmtPrice': order.lmtPrice, 'AuxPrice': order.auxPrice, 'Status': orderState.status
            }
        self.orderDf = self.orderDf.append(dictionary, ignore_index=True)
    
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {
            'Account': account, 'Symbol': contract.symbol, 'SecType': contract.secType,
            'Currency': contract.currency, 'Position': position, 'Avg cost': avgCost
        }
        self.positionDf = self.positionDf.append(dictionary, ignore_index=True)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        self.accountSummaryDf = self.accountSummaryDf.append(
            {'ReqId:': reqId, 'Account': account, 'Tag': tag, 'Value': value, 'Currency': currency},
            ignore_index=True
        )
    
    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        self.pnlDf = self.pnlDf.append(
            {'ReqId': reqId, 'DailyPnL': dailyPnL, 'UnrealizedPnL': unrealizedPnL, 'RealizedPnL': realizedPnL},
            ignore_index=True
        )
        
    #############################################
    ### EClient request functions and helpers ###
    #############################################
    def getNasdaqContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.currency = 'USD'
        contract.exchange = 'ISLAND' # alias for NASDAQ
        return contract

    def getOrder(self, orderType, action, quantity, price=None):
        order = Order()
        order.action = action
        order.orderType = orderType
        order.totalQuantity = quantity
        if orderType == 'LMT':
            order.lmtPrice = price
        elif orderType == 'STP':
            order.auxPrice = price
        return order

    def getHistoricalData(self, ticker, duration, candle_size):
        reqId = len(self.stks)
        contract = self.getNasdaqContract(ticker)
        print('\nREQUESTING HISTORICAL DATA', reqId, contract, '\n')
        # Initialize ticker in the dataframe
        self.stks = self.stks.append({'ticker': ticker, 'ticker_data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}, ignore_index=True)
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

    def placeLimitOrder(self, ticker, action, quantity, lmtPrice):
        orderId = self.nextValidOrderId
        contract = self.getNasdaqContract(ticker)
        order = self.getOrder('LMT', action, quantity, lmtPrice)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'LIMIT ORDER OF QTY:', quantity, 'AND PRICE', lmtPrice, 'FOR', ticker)
        return orderId
    
    def placeMarketOrder(self, ticker, action, quantity):
        orderId = self.nextValidOrderId
        contract = self.getNasdaqContract(ticker)
        order = self.getOrder('MKT', action, quantity)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'MARKET ORDER OF QTY:', quantity, 'FOR', ticker)
        return orderId
    
    def placeStopOrder(self, ticker, action, quantity, stopPrice):
        orderId = self.nextValidOrderId
        contract = self.getNasdaqContract(ticker)
        order = self.getOrder('STP', action, quantity, stopPrice)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'STOP ORDER OF QTY', quantity, 'AND PRICE', stopPrice, 'FOR', ticker)
    
    def cancelOrder(self, orderId):
        super().cancelOrder(orderId)
        print('\nCANCELLED ORDER WITH ID', orderId)
    
    def cancelAllOrders(self):
        self.reqGlobalCancel()
        print('\nCANCELLED ALL ORDERS')

    def getOpenOrders(self):
        self.reqOpenOrders()
    
    def getPositions(self):
        self.reqPositions()

    def getAccountSummary(self):
        self.reqAccountSummary(9000, 'All', '$LEDGER:ALL')
        
    def getPnL(self):
        self.reqPnL(9001, 'DU2017782', '')
        
#%%


# #%%
# app = IbTrader()
# app.start()

# #%%
# app.getAccountSummary()


#%%
""" tickers=['FB', 'AMZN']
for ticker in tickers:
    app.getHistoricalData(ticker, '1 W', '1 day')
"""
#app.getHistoricalData('FB', '1 Y', '1 day')
  
#%%
""" orderId = app.placeLimitOrder('AAPL', 'BUY', 1, 80)
print('Order id:', orderId) """
#%%
""" time.sleep(15)
app.cancelOrder(orderId)
print('Order', orderId, 'cancelled') """
#%%
# app.stop()
# %%
