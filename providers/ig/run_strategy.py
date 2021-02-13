# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% Run Strategy
import logging
from datetime import datetime, timedelta
from SessionMomentumTsl import SessionMomentumTsl
from live_runner import LiveRunner
#import sys
#sys.path.append('..')
#from providers.ib.IbTrader import IbTrader


# Run Back Test...
logging.basicConfig(level=logging.ERROR)
provider = 'ig'
candle_size = '5m'
strat_opts = {
  'num_warm_up_candles': 20,
  'pos_size': 0.1
}
symbol = 'CS.D.BITCOIN.CFD.IP' # 'CS.D.GBPUSD.MINI.IP'
stop_time = datetime.now() + timedelta(seconds=5)
#symbols = [symbol]
liveRunner = LiveRunner(SessionMomentumTsl, symbol, provider, candle_size, strat_opts, stop_time)
liveRunner.run()
#reporter = liveRunner(SessionMomentumTsl, symbol, provider, candle_size, strat_opts, stop_time)
# reporter.printTrades(symbol)
# reporter.printResults(symbol)
# chart = reporter.getChart(symbol)
# chart.write_to_html()

# %%
