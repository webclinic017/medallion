import plotly.graph_objects as go

class Chart():
  def __init__(self, candleDf, scatters=[], lines=[], width=800, height=600, margin=dict(l=50, r=50, b=100, t=100, pad=4)):
    df = candleDf.copy()
    #for i, row in df.iterrows():
    #  print(row)
    #df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    data = [
        go.Candlestick(x=df['date'],
          open=df['open'],
          high=df['high'],
          low=df['low'],
          close=df['close'],
          name='Candlesticks',
          increasing_line_color= 'cyan',
          decreasing_line_color= 'gray'
        )
      ]
    for scatter in scatters:
      data.append(go.Scatter(
          name=scatter['name'],
          x=scatter['x'],
          y=scatter['y'],
          mode='markers',
          marker={'symbol': scatter['symbol'],'size': 6, 'color': scatter['color']}
        ))
    for line in lines:
      data.append(go.Scatter(
        name=line['name'],
        x=line['x'],
        y=line['y'],
        mode='lines',
        marker={'line': {'color': line['color']}},
        fill='toself' if 'fill' in line and line['fill'] == True else 'none'
      ))
    fig = go.Figure(data=data)
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        #xaxis={'rangebreaks': [dict(bounds=[22, 3], pattern="hour")]},
        #autosize=False,
        #width=width,
        #height=height,
        margin=margin,
        dragmode='pan'
      )
    fig.update_xaxes(
        #type="category",
        rangebreaks=[
          dict(bounds=["sat", "mon"]), #hide weekends
          #dict(values=['2020-12-25', '2020-12-26', '2020-12-27']),
          dict(bounds=[22, 15], pattern="hour"), #hide hours outside of 9am-5pm
        ]
      )
    self.fig = fig
    # returns function that receives file name to write the html to 

  def show(self):
    config = {
        'scrollZoom': True,
        'displaylogo': False,
        'modeBarButtonsToAdd': ['drawline', 'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape']
    }
    self.fig.show(config=config)

  def write_to_html(self, filename = 'chart.html'):
    self.fig.write_html(filename)
