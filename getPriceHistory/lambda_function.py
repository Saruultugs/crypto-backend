# --function-name getPriceHistory

config = {
  'db': 'currency_prices',
  'url': 'https://api.coinmarketcap.com/v1/ticker/?limit=1',
  'fresh_build': False,
  'clear_build': False
}
log = ['Running loadPriceHistoryToRDS']

def getDataFromSource():
  log.append('Getting data from ' + config['url'])
  return json.loads(requests.get(config['url']).content)

def getMySql():
  c = None
  try:
    # c = pymysql.connect(config['rds_host'], user=config['user'], passwd=config['passwd'], db=config['db'], connect_timeout=5)
    c = pymysql.connect('currency-prices.coeb0qsth1lu.us-west-1.rds.amazonaws.com', user='shazrat', passwd='Palmpre3', db='currency_prices', connect_timeout=5)
    log.append('Connected RDS mySQL :)')
  except:
    log.append('Error connecting to RDS mySQL')
    # sys.exit()
  return c

# def createTable(connection):
#     connection.cursor.execute('CREATE TABLE BTC (time BIGINT NOT NULL, price_usd FLOAT, price_btc FLOAT, 24h_volume_usd BIGINT, market_cap_usd BIGINT, available_supply BIGINT, total_supply BIGINT, PRIMARY KEY (time))')

# def insertRow(connection, data):
#     mapping = {
#         'last_updated': 'time',
#         'price_usd': 'price_usd',
#         'price_btc': 'price_btc',
#         '24h_volume_usd': '24h_volume_usd',
#         'market_cap_usd': 'market_cap_usd',
#         'available_supply': 'available_supply',
#         'total_supply': 'total_supply'
#     }
#     log = ['Writing currency price data to RDS']
#     query = ("INSERT INTO BTC (time, price_usd, price_btc, 24h_volume_usd, market_cap_usd, available_supply, total_supply) VALUES (data.last_updated, data.price_usd, data.price_btc, data.24h_volume_usd, data.market_cap_usd, data.available_supply, data.total_supply)")
#     try:
#         connection.cursor.execute(query)
#         connection.commit()
#     except:
#         logger.error('Error when inserting row')
#     return log

# def showARow(cursor):
#     cursor.execute('select * from BTC')
#     return cursor.fetchone()

# def showAllRows(cursor):
#     cursor.execute('select * from BTC')
#     return cursor.fetchall()

# def showTables(cursor):
#     cursor.execute('SHOW TABLES')
#     return cursor.fetchall()

# def lambda_handler(event, context):
#     log = ['Starting main lambda tasks.']
#     data = getDataFromSource()
#     connection = getMySql()
#     log.append(insertRow(connection, data[0]))
#     return log

def main():
  # data = getDataFromSource()
  db = getMySql()

def lambda_handler(event, context):
  from firebase import firebase
  import json
  import logging
  import pymysql
  import requests
  import sys
  main()
  return log

if __name__ == '__main__':
  import os
  os.system('clear')
  print 'Building and deploying package to Lambda'
  if config['fresh_build']:
    os.system('ls | grep -v lambda_function.py | xargs rm -r')
  if not os.path.exists('requests'):
    os.system('pip install requests -t .')
  if not os.path.exists('firebase'):
    os.system('pip install python-firebase -t .')
  if not os.path.exists('pymysql'):
    os.system('pip install pymysql -t .')
  os.system('zip -rq deployment-package.zip ./*')
  os.system('aws lambda update-function-code --function-name loadPriceHistoryToRDS --zip-file fileb://./deployment-package.zip')
  if config['clear_build']:
    os.system('ls | grep -v lambda_function.py | xargs rm -r')
  print 'Running function'
  os.system('aws lambda invoke --function-name loadPriceHistoryToRDS lambda_return.txt')
  os.system('clear')
  print 'Lambda return:'
  os.system('cat lambda_return.txt')
  os.system('rm lambda_return.txt')
  print
