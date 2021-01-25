import logging

class FakeTrader():
  def __init__(self, init_balance):
    self.orders = []
    self.positions = []
    self.next_order_id = 0
    self.next_pos_id = 0
    self.trades = {}
    self.balance = init_balance
    self.num_trades = 0
    self.num_wins = 0
    self.total_r = 0

  def placeLmtBracketOrder(self, symbol, action, quantity, lmt_price, tp_price, sl_price, placed):
    order = {
      'symbol': symbol,
      'action': action,
      'quantity': quantity,
      'type': 'limit',
      'price': lmt_price,
      'placed': placed,
      'bracket': [
        {'type': 'sl', 'price': sl_price},
        {'type': 'tp', 'price': tp_price}
      ],
      'id': self.next_order_id
    }
    self.orders.append(order)
    self.next_order_id += 1
    logging.debug(f'{placed} {symbol} Placed order. quantity: {quantity} lmt_price: {lmt_price} tp: {tp_price} sl: {sl_price}')
    return order['id']

  def placeMktBracketOrder(self, symbol, action, quantity, mkt_price, tp_price, sl_price, time_opened):
    if self.balance - quantity >= 0:
      # Open position at market price
      position = {
        'id': self.next_pos_id,
        'symbol': symbol,
        'action': action,
        'quantity': quantity,
        'price_bought': mkt_price,
        'time_opened': time_opened,
        'orig-sl': sl_price if sl_price else None # To calculate later R/R
      }
      self.positions.append(position)
      self.next_pos_id += 1
      # Place stop loss order
      self.orders.append({
        'symbol': symbol,
        'action': 'sell' if action == 'buy' else 'buy',
        'quantity': quantity,
        'type': 'sl',
        'price': sl_price,
        'id': self.next_order_id,
        'time_placed': time_opened
        })
      # Place take profit order
      self.next_order_id += 1
      self.orders.append({
        'symbol': symbol,
        'action': 'sell' if action == 'buy' else 'buy',
        'quantity': quantity,
        'type': 'tp',
        'price': tp_price,
        'time': time_opened,
        'id': self.next_order_id
        })
      self.next_order_id += 1
      logging.debug(f'{time_opened} {symbol} Placed order. quantity: {quantity} lmt_price: {mkt_price} tp: {tp_price} sl: {sl_price}')
    else:
      # order['status'] = 'cancelled'
      print(f'{symbol} Cannot open position {time_opened}. Not enough funds')

  def closePosition(self, pos_id, price_sold, date, reason=None):
    def find_position_idx(pos_id):
      for idx, pos in enumerate(self.positions):
        if pos['id'] == pos_id:
          return idx
      return None

    pos_idx = find_position_idx(pos_id)
    if pos_idx == None:
      print('Cannot close position. Id', pos_id, 'not found. Current positions', self.positions)
      return
    position = self.positions[pos_idx]
    trade = {
      'symbol': position['symbol'],
      'price_bought': position['price_bought'],
      'price_sold': price_sold,
      'orig_sl': position['orig_sl'],
      # 'diff': order['price_bought'] - order['sl_price'],
      'time_opened': position['time_opened'],
      'time_closed': date,
      'quantity': position['quantity'],
      'reason': reason
    }
    trade['raw_profit'] = trade['price_sold'] - trade['price_bought']
    trade['raw_profit_mlt'] = trade['raw_profit'] / trade['price_bought'] + 1
    trade['profit'] = trade['quantity'] * trade['raw_profit']
    trade['return'] = trade['quantity'] * trade['raw_profit_mlt']
    #trade['profit'] = (trade['price_sold'] * order['quantity']) - (trade['price_bought'] * order['quantity'])
    #print('Profit:', trade['profit'], 'Sold:', trade['price_sold'], 'Bought', trade['price_bought'], 'Qty:', order['quantity'])
    #trade['profit_mlt'] = trade['profit'] / (trade['price_bought'] * order['quantity']) + 1
    trade['r'] = (price_sold - trade['price_bought']) / (trade['price_bought'] - trade['orig_sl']) if trade['orig_sl'] else None
    #if trade['raw_profit'] > 0 else -1
    #print('R', trade['r'], 'Total R:', self.total_r, reason)
    self.total_r = (self.total_r + trade['r']) if trade['r'] != None else None

    self.positions.pop(pos_idx)
    self.trades[trade['symbol']].append(trade)
    self.balance += trade['raw_profit'] * trade['quantity']
    self.num_trades += 1
    self.num_wins += 1 if trade['raw_profit'] > 0 else 0
    logging.debug(f'{date} {trade["symbol"]} Close position. Profit: {trade["raw_profit"] * trade["quantity"]} Balance: {self.balance} NumPos: {len(self.positions)}')

  def openPosition(self, symbol, action, quantity, price, time_opened, sl_price=None):
    position = {
      'id': self.next_pos_id,
      'symbol': symbol,
      'action': action,
      'quantity': quantity,
      'price_bought': price,
      'time_opened': time_opened,
      'orig_sl': sl_price # To calculate later R/R
    }
    self.next_pos_id += 1
    if self.balance - position['quantity'] * position['price_bought'] >= 0:
      self.positions.append(position)
      logging.debug(f'{time_opened} {position["symbol"]} Open position. Num positions open: {len(self.positions)}')
      return position['id']
    else:
      logging.debug(f'{time_opened} {position["symbol"]} Could not open position on {time_opened}. Not enough funds')
      return None

  def closeAllPositions(self, candle, reason=None):
    logging.debug('\nClose all positions')
    for position in self.positions:
      self.closePosition(position['id'], candle.close, candle.date)
  
  def position_open(self):
    return bool(len(self.positions))
  
  def position_open_symbol(self, symbol):
    for position in self.positions:
      if position['symbol'] == symbol:
        return True
    return False
