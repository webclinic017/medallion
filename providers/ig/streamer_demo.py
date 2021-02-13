#%%
import os
import sys
import logging
import traceback
from ig_streamer import Subscription, LSClient, wait_for_input
from ig_api import IgApi

ig = IgApi()
ig.login()


logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(levelname)-7s ' +
                    '%(threadName)-15s %(message)s', level=logging.INFO)

# Establishing a new connection to Lightstreamer Server
print("Starting connection")
# lightstreamer_client = LSClient("http://localhost:8080", "DEMO")
#lightstreamer_client = LSClient("http://push.lightstreamer.com", "DEMO")
url = ig.lightstreamer_url
user = os.environ.get('IG_API_KEY')
password = "CST-" + ig.cst + "|XST-" + ig.x_security_token
lightstreamer_client = LSClient(url, user=user, password=password)
try:
    lightstreamer_client.connect()
except Exception as e:
    print("Unable to connect to Lightstreamer Server")
    print(traceback.format_exc())
    sys.exit(1)


# Making a new Subscription in MERGE mode
epic = "MARKET:CS.D.EURUSD.MINI.IP"
subscription = Subscription(
    mode="MERGE",
    items=[epic],
    fields=["BID", "OFFER", "MARKET_STATE", "UPDATE_TIME"])


# A simple function acting as a Subscription listener
def on_item_update(item_update):
    print("BID: {BID:<7}; OFFER: {OFFER:<7}; MARKET_STATE: {MARKET_STATE:<9}; UPDATE_TIME: {UPDATE_TIME:<8}".format(**item_update["values"]))

# Adding the "on_item_update" function to Subscription
subscription.addlistener(on_item_update)

# Registering the Subscription
sub_key = lightstreamer_client.subscribe(subscription)

wait_for_input()

# Unsubscribing from Lightstreamer by using the subscription key
lightstreamer_client.unsubscribe(sub_key)

# Disconnecting
lightstreamer_client.disconnect()

#%%