#%%
import os
import sys
import logging
import traceback
from ig_streamer import Subscription, LSClient
from ig_api import IgApi

class IgTrader(IgApi):
  def __init__(self):
    self.positions = []
    self.pos_to_confirm = []

  def connect(self):
    self.login()
    logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(levelname)-7s ' +
                    '%(threadName)-15s %(message)s', level=logging.INFO)
    print("Starting live connection")
    user = os.environ.get('IG_API_KEY')
    password = "CST-" + self.cst + "|XST-" + self.x_security_token
    self.lightstreamer_client = LSClient(self.lightstreamer_url, user=user, password=password)
    try:
      self.lightstreamer_client.connect()
    except Exception as e:
      print("Unable to connect to Lightstreamer Server")
      print(traceback.format_exc())
      sys.exit(1)

  def subscribeToMarketData(self, epics, on_tick):
    return self._subscribe(
        mode="MERGE",
        items=list(map(lambda epic: f'MARKET:{epic}', epics)),
        fields=["BID", "OFFER", "MARKET_STATE", "UPDATE_TIME"],
        callback=on_tick
    )

  def subscribeToTickData(self, epics, on_tick):
    return self._subscribe(
        mode="DISTINCT",
        items=list(map(lambda epic: f'CHART:{epic}:TICK', epics)),
        fields=["BID", "OFR", "UTM"],
        callback=on_tick
    )

  def subscribeToCandleData(self, epics, candle_size, on_candle):
    """ possible candle sizes: SECOND, 1MINUTE, 5MINUTE, HOUR  """
    return self._subscribe(
      mode='MERGE',
      items=list(map(lambda epic: f'CHART:{epic}:{candle_size}', epics)),
      fields=['LTV', 'UTM', 'OFR_OPEN', 'OFR_HIGH', 'OFR_LOW', 'OFR_CLOSE', 'BID_OPEN', 'BID_HIGH', 'BID_LOW', 'BID_CLOSE', 'CONS_END'],
      callback=on_candle
    )

  def subscribeToAccountStatus(self, on_account_update):
    return self._subscribe(
      mode='MERGE',
      items=[f'ACCOUNT:{self.account_id}'],
      fields=['PNL', 'AVAILABLE_CASH', 'FUNDS', 'MARGIN', 'EQUITY', 'EQUITY_USED'],
      callback=on_account_update
    )

  def subscribeToTradeUpdates(self, on_trade_update):
    """ A TRADE CONFIRMATION (type: str):
    {"direction":"BUY","epic":"CS.D.BITCOIN.CFD.IP","stopLevel":46526.90000,"limitLevel":null,
    "dealReference":"LHVRB3J6S8N44TD","dealId":"DIAAAAE6FQXXBAZ","limitDistance":null,"stopDistance":null,
    "expiry":"-","affectedDeals":[{"dealId":"DIAAAAE6FQXXBAZ","status":"OPENED"}],
    "dealStatus":"ACCEPTED","guaranteedStop":false,"trailingStop":true,"level":46996.9,
    "reason":"SUCCESS","status":"OPEN","size":0.1,"profit":null,"profitCurrency":null,
    "date":"2021-02-10T09:25:52.084","channel":"PublicRestOTC"}
    AN OPEN POSITION UPDATE (type: str):
    {"dealReference":"JK4PR9ELBME44S3","dealId":"DIAAAAE6FQKP3AV","direction":"BUY","epic":"CS.D.BITCOIN.CFD.IP",
    "status":"OPEN","dealStatus":"ACCEPTED","level":46946.2,"size":0.1,"timestamp":"2021-02-10T09:23:35.000",
    "channel":"PublicRestOTC","dealIdOrigin":"DIAAAAE6FQKP3AV","expiry":"-","stopLevel":null,"limitLevel":null,
    "guaranteedStop":false}
    OR:
    {"dealReference":"LHVRB3J6S8N44TD","dealId":"DIAAAAE6FQXXBAZ","direction":"BUY","epic":"CS.D.BITCOIN.CFD.IP",
    "status":"DELETED","dealStatus":"ACCEPTED","level":46526.9,"size":0,"timestamp":"2021-02-10T09:58:20.241",
    "channel":"PublicRestOTC","dealIdOrigin":"DIAAAAE6FQXXBAZ","expiry":"-","stopLevel":46526.9,"limitLevel":null,
    "guaranteedStop":false}
    """
    return self._subscribe(
      mode='DISTINCT',
      items=[f'TRADE:{self.account_id}'],
      fields=['CONFIRMS', 'OPU', 'WOU'],
      callback=on_trade_update
    )

  def _subscribe(self, mode, items, fields, callback):
    subscription = Subscription(mode=mode, items=items, fields=fields)
    subscription.addlistener(callback)
    return self.lightstreamer_client.subscribe(subscription)


  def unsubscribe(self, sub_key):
    # Unsubscribing from Lightstreamer by using the subscription key
    self.lightstreamer_client.unsubscribe(sub_key)

  def disconnect(self):
    # Disconnecting
    self.lightstreamer_client.disconnect()


#%%
"""
trader = IgTrader()
trader.connect()

#%%
def onAccountUpdate(data):
  print('ACCOUNT UPDATE: ', data)
    
import json
def onTradeUpdate(data):
  vals = data['values']
  if 'CONFIRMS' in vals and vals['CONFIRMS'] != None:
    confirmation = json.loads(vals['CONFIRMS'])
    if confirmation['dealStatus'] == 'REJECTED':
      print(f"TRADE REJECTED. REASON: {confirmation['reason']}")
  if 'OPU' in vals and vals['OPU'] != None:
    open_pos = json.loads(vals['OPU'])
    if open_pos['status'] == 'OPEN':
      print(f"POSITION OPENED. Level: {open_pos['level']}; Direction: {open_pos['direction']}; Size: {open_pos['size']}; Time: {open_pos['time']}")

def onTradeUpdate(data):
  vals = data['values']
  print('onTradeUpdate')
  if 'CONFIRMS' in vals:
    print(f"TRADE CONFIRMATION: {vals['CONFIRMS']}")
  if 'OPU' in vals:
    print(f"OPEN POSITION UPDATE: {vals['OPU']}")
  if 'WOU' in vals:
    print(f"WORKING ORDER UPDATE: {vals['WOU']}")

trade_sub = trader.subscribeToTradeUpdates(onTradeUpdate)
acc_sub = trader.subscribeToAccountStatus(onAccountUpdate)
#%%
order = {
  'currencyCode': 'USD',
  'direction': 'BUY',
  'epic': 'CS.D.BITCOIN.CFD.IP',
  'level': '39500',
  'size': '1',
  'type': 'STOP'
}
deal_ref = trader.createOrder(order)

#%% OPEN MARKET POSITION
position = {
  'currencyCode': 'USD',
  'direction': 'BUY',
  'epic': 'CS.D.GBPUSD.MINI.IP', #'CS.D.BITCOIN.CFD.IP',
  'orderType': 'MARKET',
  'size': 1,
  'trailingStop': False
}
deal_ref = trader.openPosition(position)
confirmation = trader.confirmDeal(deal_ref)
deal_id = confirmation['deal_id'] if 'deal_id' in confirmation else None

#%% OPEN MARKET POSITION WITH TRAILING STOP
position = {
  'currencyCode': 'USD',
  'direction': 'BUY',
  'epic': 'CS.D.BITCOIN.CFD.IP',
  'orderType': 'MARKET',
  'size': 0.1,
  'trailingStop': True,
  'stopDistance': 200,
  'trailingStopIncrement': 20
}
deal_ref = trader.openPosition(position)
confirmation = trader.confirmDeal(deal_ref)
print('DEAL CONFIRMATION:', confirmation)

#%% close position

position = {
  'dealId': deal_id,
  'direction': 'SELL',
  'orderType': 'MARKET',
  'size': 1
}
result = trader.closePosition(position)
confirmation = trader.confirmDeal(result['deal_ref'])
print('DEAL CONFIRMATION:', confirmation)

#%%
trader.confirmDeal(deal_ref)
#%%
trader.unsubscribe(trade_sub)
#trader.unsubscribe(sub_key2)
trader.disconnect()
# %%
"""