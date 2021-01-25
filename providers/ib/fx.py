from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import asyncio
import pandas as pd

class IbTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.historicalDataReqs = {} # dict to track state of requests
        self.stks = {}
    
    async def start(self):
        self.started = threading.Event()
        print('\nCONNECTING...')
        self.connect('127.0.0.1', 7497, clientId=1)

        self.appFinished = threading.Event()
        threading.Thread(target=self.run_app).start()

        threading.Thread(target=self.stop_app).start()
        await asyncio.sleep(1)
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
        
    def contractDetails(self, reqId, contractDetails):
        print(f'reqId: {reqId}; contract: {contractDetails}')
        
    def historicalData(self, reqId, bar):
        ticker = list(filter(lambda key: self.stks[key]['reqId'] == reqId, self.stks.keys()))[0]
        self.stks[ticker]['data'] = self.stks[ticker]['data'].append({'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume}, ignore_index=True)
        #print(f'{self.stks.iloc[reqId].ticker} ', end='')
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)

    def historicalDataEnd(self, reqId, start, end):
        ticker = list(filter(lambda key: self.stks[key]['reqId'] == reqId, self.stks.keys()))[0]
        self.historicalDataReqs[reqId].set()
        #print(f'historicalDataEnd. reqId: {self.stks[ticker]["reqId"]} ticker: {ticker}')
    
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print('nextValidOrderId:', orderId)
        self.started.set()
    
        
    #############################################
    ### EClient request functions and helpers ###
    #############################################
    def getNasdaqContract(self, symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.currency = 'USD'
        contract.exchange = 'ISLAND' # alias for NASDAQ vc x
        return contract

    def getFxContract(self, symbol):
        symbolParts = symbol.split('.')
        if len(symbolParts) != 2:
            raise Exception('Invalid FX symbol. Must be in format XXX.YYY')
        contract = Contract()
        contract.symbol = symbolParts[0]
        contract.secType = 'CASH'
        contract.currency = symbolParts[1]
        contract.exchange = 'IDEALPRO'
        return contract
    
    def getContract(self, symbol, exchange):
        return {
            'NASDAQ': self.getNasdaqContract,
            'FX': self.getFxContract
        }[exchange](symbol)

    async def getHistoricalData(self, ticker, exchange, duration, candle_size, endDateTime=''):
        print('reqHistoricalData')
        reqId = len(self.stks)
        self.historicalDataReqs[reqId] = threading.Event()
        contract = self.getContract(ticker, exchange)
        self.stks[ticker] = {'reqId': reqId, 'data': pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])}
        """ self.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime=endDateTime, # empty string = current time
            durationStr=duration,
            barSizeSetting=candle_size,
            whatToShow='TRADES',
            useRTH=1,
            formatDate=1,
            keepUpToDate=0,
            chartOptions=[]) """
        contract = Contract()
        contract.symbol = 'EUR'
        contract.secType = 'CASH'
        contract.currency = 'USD'
        contract.exchange = 'IDEALPRO'
        self.reqHistoricalData(reqId=reqId, 
                      contract=self.getContract(ticker, exchange),
                      endDateTime=endDateTime,
                      durationStr='1 W',
                      barSizeSetting='1 day',
                      whatToShow='MIDPOINT',
                      useRTH=1,
                      formatDate=1,
                      keepUpToDate=0,
                      chartOptions=[])
        self.historicalDataReqs[reqId].wait()
        self.historicalDataReqs[reqId].clear()
        self.historicalDataReqs.pop(reqId)
        print(f'getHistorialData. Got {ticker} data') #. Num stocks still to retrieve: {len(self.historicalDataReqs)}')
        return self.stks[ticker]


async def main():
  ib = IbTrader()
  await ib.start()

  await ib.getHistoricalData(
    ticker='EUR.USD',
    exchange='FX',
    duration='1 W',
    candle_size='1 day',
    endDateTime=''
  )

  print('\nDONE!\nThis is what we got:')
  print(ib.stks)

  
  ib.stop()

if __name__ == '__main__':
  try:
    asyncio.run(main())
  except Exception as e:
    print('Error executing main:', e)