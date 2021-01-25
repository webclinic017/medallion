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
from datetime import datetime

class IbTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.barCount = 0
        self.historicalDataReqs = {} # dict to track state of requests
        self.fundamentalDataReq = threading.Event()
        self.fundamentalData = {}
        self.stks = {}
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
        # Events
        self.evAppStarted = threading.Event()
        self.evAppFinished = threading.Event()
        self.evServerResponded = threading.Event()
    
    def start(self):
        print('\nCONNECTING...')
        self.connect('127.0.0.1', 7497, clientId=1)

        threading.Thread(target=self.run_app).start()

        threading.Thread(target=self.stop_app).start()
        self.evAppStarted.wait()
        print('READY!\n')

    def stop(self):
        self.evAppFinished.set()

    def run_app(self):
        self.run()

    def stop_app(self):
        self.evAppFinished.wait()
        #if self.appFinished.is_set():
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
            if errorCode == 162: # No historical data for a particar symbol
                self.evServerResponded.set()
        
    def contractDetails(self, reqId, contractDetails):
        print(f'reqId: {reqId}; contract: {contractDetails}')
        
    def symbolSamples(self, reqId: int, contractDescriptions):
        super().symbolSamples(reqId, contractDescriptions)
        print("symbolSamples. Search Results:")
        for contractDescription in contractDescriptions:
            derivSecTypes = ""
            for derivSecType in contractDescription.derivativeSecTypes:
                derivSecTypes += derivSecType
                derivSecTypes += " "
                print("Contract: conId:%s, symbol:%s, secType:%s primExchange:%s, currency:%s, derivativeSecTypes:%s" % (
                    contractDescription.contract.conId,
                    contractDescription.contract.symbol,
                    contractDescription.contract.secType,
                    contractDescription.contract.primaryExchange,
                    contractDescription.contract.currency, derivSecTypes))
        
        self.evServerResponded.set()
        
    def historicalData(self, reqId, bar):
        self.barCount += 1
        ticker = list(filter(lambda key: self.stks[key]['reqId'] == reqId, self.stks.keys()))[0]
        #print('historicalData:', ticker, self.stks[ticker])
        self.stks[ticker]['data'] = self.stks[ticker]['data'].append({'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume}, ignore_index=True)
        #print(f'{self.stks[reqId].ticker} ', end='')
        #print('barCount:', self.barCount)

    def historicalDataEnd(self, reqId, start, end):
        ticker = list(filter(lambda key: self.stks[key]['reqId'] == reqId, self.stks.keys()))[0]
        #self.historicalDataReqs[reqId].set()
        self.evServerResponded.set()
        #print(f'historicalDataEnd. reqId: {self.stks[ticker]["reqId"]} ticker: {ticker}')
    
    def fundamentalData(self, reqId, data):
        super().fundamentalData(reqId, data)
        print("FundamentalData. ReqId:", reqId, "Data:", data)
        self.fundamentalData[reqId] = data
        #print("FundamentalData. ReqId:", reqId, "Data:", data)
        #ticker = list(filter(lambda key: self.fundamentalData[key]['reqId'] == reqId, self.fundamentalData.keys()))[0]
        #self.fundamentalData['ticker']['data'] = data
        #self.fundamentalDataReq.set()
    
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print('nextValidOrderId:', orderId)
        self.evAppStarted.set()
    
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
        print(f'openOrder. orderId: {orderId} contract: {contract.symbol} orderState: {orderState.status}')
    
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled, "Remaining:", remaining, "AvgFillPrice:", avgFillPrice, "PermId:", permId, "ParentId:", parentId, "LastFillPrice:", lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld, "MktCapPrice:", mktCapPrice)

    def openOrderEnd(self):
        super().openOrderEnd()
        print("OpenOrderEnd")
    
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
        self.evServerResponded.set()

    def accountSummaryEnd(self, reqId):
        super().accountSummaryEnd(reqId)
        print('AccountSummaryEnd. ReqId:', reqId)
        self.evServerResponded.set()
    
    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        self.pnlDf = self.pnlDf.append(
            {'ReqId': reqId, 'DailyPnL': dailyPnL, 'UnrealizedPnL': unrealizedPnL, 'RealizedPnL': realizedPnL},
            ignore_index=True
        )
    
    ###############
    ### Helpers ###
    ###############
    def await_server_response(self):
        self.evServerResponded.wait()
        self.evServerResponded.clear()

    def getNasdaqContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.currency = 'USD'
        contract.exchange = 'ISLAND' # alias for NASDAQ
        return contract

    def getFxContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol[0:3]
        contract.secType = 'CASH'
        contract.currency = symbol[3:]
        contract.exchange = 'IDEALPRO'
        return contract
    
    def getCfdContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'CFD'
        contract.currency = symbol[3:]
        contract.exchange = 'SMART'
        return contract
    
    def getContract(self, symbol, exchange):
        return {
            'NASDAQ': self.getNasdaqContract,
            'FX': self.getFxContract,
            'CFD': self.getCfdContract
        }[exchange](symbol)
        
    #################################
    ### EClient request functions ###
    #################################

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

    def getHistoricalData(self, ticker, exchange, duration, candle_size, endDateTime=''):
        """ endDateTime:str - Defines a query end date and time at any point during the past 6 mos.
            Valid values include any date/time within the past six months in the format:
            yyyymmdd HH:mm:ss ttt           NB. Tried with date more than a year ago and it works"""
        reqId = len(self.stks)
        self.historicalDataReqs[reqId] = threading.Event()
        contract = self.getContract(ticker, exchange)
        #print('REQUESTING HISTORICAL DATA. reqId:', reqId, 'Contract:', contract)
        # Initialize ticker
        self.stks[ticker] = {'reqId': reqId, 'data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}
        self.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime=endDateTime, # empty string = current time
            durationStr=duration,
            barSizeSetting=candle_size,
            whatToShow='MIDPOINT' if exchange == 'FX' else 'TRADES',
            useRTH=1,
            formatDate=1,
            keepUpToDate=0,
            chartOptions=[])
        #await asyncio.sleep(1)
        #print('getHistoricalData. reqId before wait:', reqId)
        #self.historicalDataReqs[reqId].wait()
        #self.historicalDataReqs[reqId].clear()
        #self.historicalDataReqs.pop(reqId)
        self.await_server_response()
        print(f'getHistorialData. Got {ticker} data') #. Num stocks still to retrieve: {len(self.historicalDataReqs)}')
        return self.stks[ticker]['data']
        
    def getFundamentalData(self, ticker, reportType):
        '''reportType:str - One of the following XML reports:
            ReportSnapshot (company overview)
            ReportsFinSummary (financial summary)
            ReportRatios (financial ratios)
            ReportsFinStatements (financial statements)
            RESC (analyst estimates)
            CalendarReport (company calendar)
        '''
        #reqId = len(self.fundamentalData)
        #contract = self.getNasdaqContract(ticker)
        contract = Contract()
        contract.symbol = 'AAPL'
        contract.secType = 'STK'
        contract.currency = 'USD'
        contract.exchange = 'ISLAND'
        #self.fundamentalData[ticker] = {'reqId': reqId}
        #self.fundamentalDataReq.clear()
        #print('\nREQUEST:')
        #print(reqId)
        #print(contract)
        #print(reportType)
        self.reqFundamentalData(
            reqId=0,
            contract=contract,
            reportType='ReportsFinStatements',
            fundamentalDataOptions=[])
        #await asyncio.sleep(1)
        #self.fundamentalDataReq.wait()
        #return self.fundamentalData[ticker]['data']

    def placeLimitOrder(self, ticker, exchange, action, quantity, lmtPrice):
        orderId = self.nextValidOrderId
        contract = self.getContract(ticker, exchange)
        order = self.getOrder('LMT', action, quantity, lmtPrice)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'LIMIT ORDER OF QTY:', quantity, 'AND PRICE', lmtPrice, 'FOR', ticker)
        return orderId
    
    def placeMarketOrder(self, ticker, exchange, action, quantity):
        orderId = self.nextValidOrderId
        contract = self.getContract(ticker, exchange)
        order = self.getOrder('MKT', action, quantity)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'MARKET ORDER OF QTY:', quantity, 'FOR', ticker)
        return orderId
    
    def placeStopOrder(self, ticker, exchange, action, quantity, stopPrice):
        orderId = self.nextValidOrderId
        contract = self.getContract(ticker, exchange)
        order = self.getOrder('STP', action, quantity, stopPrice)
        self.placeOrder(orderId, contract, order)
        self.ordersPlaced.append(orderId)
        print('\nPLACED', action, 'STOP ORDER OF QTY', quantity, 'AND PRICE', stopPrice, 'FOR', ticker)
    
    def cancelOrder(self, orderId):
        super().cancelOrder(orderId)
        print('\nCANCELLED ORDER WITH ID', orderId)
    
    def cancelAllOrders(self):
        print('\nCANCELLED ALL ORDERS')

    def getOpenOrders(self):
        self.reqOpenOrders()
    
    def getPositions(self):
        self.reqPositions()

    async def getAccountSummary(self):
        self.reqAccountSummary(9000, 'All', '$LEDGER:ALL')
        self.await_server_response()
        print('getAccountSummary. Finished')
        
    def getPnL(self):
        self.reqPnL(9001, 'DU2017782', '')

    def symbolSearch(self, searchText):
        self.reqMatchingSymbols(211, searchText)
        self.evServerResponded.wait()
        self.evServerResponded.clear()
        

# %%
