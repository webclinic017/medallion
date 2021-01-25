class Plotter():
  """This Plotter class is to be used by the strategy"""
  def __init__(self, charts):
    self.data = {}
    for chart in charts:
      self.data[chart] = {
        'lines': [],
        'scatter_pts': []
      }

  def createLine(self, chart: str, name: str, color: str):
    self.data[chart]['lines'].append({'name': name, 'color': color, 'x': [], 'y': []})
  
  def addPtToLine(self, chart: str, name: str, x, y):
    line = [l for l in self.data[chart]['lines'] if name == l['name']][0]
    line['x'].append(x)
    line['y'].append(y)

  def createPointsGroup(self, chart: str, name: str, color: str, style: str):
    """style could be "cross" or "circle", for example. See plotly docs for more options"""
    self.data[chart]['scatter_pts'].append({'name': name, 'color': color, 'symbol': style, 'x': [], 'y': []})

  def pointsGroupExists(self, chart, group_name):
    exists = False
    for group in self.data[chart]['scatter_pts']:
      if group['name'] == group_name:
        exists = True
    return exists

  def addPtToPointsGroup(self, chart, name: str, x, y):
    group = [g for g in self.data[chart]['scatter_pts'] if name == g['name']][0]
    group['x'].append(x)
    group['y'].append(y)
