from functools import reduce
import plotly.graph_objects as go
from datetime import datetime

class chainList(list):
  def __init__(self, l):
    list.__init__(self,l)
  def map(self, f):
    return chainList(map(f, self[:]))
  def filter(self, f):
    return chainList(filter(f, self[:]))
  def reduce(self, f, i=None):
    if i == None:
      return reduce(f, list(self[:]))
    else:
      return reduce(f, list(self[:]), i)


def chart(df, width=800, height=600, margin=dict(l=50, r=50, b=100, t=100, pad=4)):
    df = df.copy()
    #df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    fig = go.Figure(
        data=[
          go.Candlestick(x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'])
        ])
    config = dict({
        'scrollZoom': True,
        'displaylogo': False,
        'modeBarButtonsToAdd': ['drawline', 'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape']
    })
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        autosize=False,
        width=width,
        height=height,
        margin=margin,
        dragmode='pan'
      )
    fig.show(config=config)