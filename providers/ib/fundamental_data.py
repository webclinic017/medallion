from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import pandas as pd

class IbTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.started = threading.Event()
        self.appFinished = threading.Event()
        self.gotFundamentalData = threading.Event()
    
    def start(self):
        print('\nCONNECTING...')
        self.connect('127.0.0.1', 7497, clientId=1)

        threading.Thread(target=self.run_app).start()

        threading.Thread(target=self.stop_app).start()
        self.started.wait()
        print('READY!\n')

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
    
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print('nextValidOrderId:', orderId)
        self.started.set()
    
    def fundamentalData(self, reqId, data):
        super().fundamentalData(reqId, data)
        print("FundamentalData. ReqId:", reqId, "Data:", data)
        self.fundamentalData = data
        self.gotFundamentalData.set()

    
        
    #############################################
    ### EClient request functions and helpers ###
    #############################################
    def getFundamentalData(self, reqId, contract, reportType):
        self.reqFundamentalData(
            reqId=0,
            contract=contract,
            reportType=reportType,
            fundamentalDataOptions=[])
        self.gotFundamentalData.wait()
        return self.fundamentalData


ib = IbTrader()
ib.start()

contract = Contract()
contract.symbol = 'AAPL'
contract.secType = 'STK'
contract.currency = 'USD'
contract.exchange = 'ISLAND'
fundamentalData = ib.getFundamentalData(0, contract, reportType='ReportsFinStatements')

print('\nDONE!\nThis is what we got:')
print(fundamentalData)

ib.stop()
