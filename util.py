from functools import reduce

class chainList(list):
  def __init__(self, l):
    list.__init__(self,l)
  def map(self, f):
    return chainList(map(f, self[:]))
  def filter(self, f):
    return chainList(filter(f, self[:]))
  def reduce(self, f, i):
    print('start reduce')
    return chainList(reduce(f, self[:], i))
