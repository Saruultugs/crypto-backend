# --function-name getPriceHistory

# set global variables (for both build and runtime)
config = {
    'function_name': 'getPriceHistory',
    'source_url': 'https://api.bitcoincharts.com/v1/csv/',
    'db': 'currency_prices',
    'url': 'https://api.coinmarketcap.com/v1/ticker/?limit=1',
    'fresh_build': False,
    'clear_build': True
}
log = ['Running ' + config['function_name']]

def temp():
    import gzip
    import urllib2
    import StringIO
    from firebase import firebase

    def dataFromUrlGzCsv(url):
        # headers: timestamp, price, volume
        compressedFile = StringIO.StringIO()
        compressedFile.write(urllib2.urlopen(url).read())
        compressedFile.seek(0)
        decompressedFile = gzip.GzipFile(fileobj=compressedFile, mode='rb')
        return [d.strip('\n') for d in decompressedFile.readlines()]

    def getFirebaseApplication():
        return firebase.FirebaseApplication('https://crypto-trading-953aa.firebaseio.com/', None)

    def getFBRoot(fba):
        return fba.get('/', None)

    def postPrice(fba, currency, datetime, price, volume, exchange):
        return fba.post('/currency/', {})

    def removeFBKey(fba, key):
        return fba.delete(key, None)

    def updateStatus(fba):
        log = ['Updating ping status of major domains']
        domains = ['google', 'amazon', 'twitter', 'reddit', 'facebook']
        for domain in domains:
            log.append({'domain': domain, 'status': fba.put('/status', domain, '1')})
        return log

    def writePrices(fba):
        log = ['Writing historic currency price data']
        dataUrl = 'https://api.bitcoincharts.com/v1/csv/localbtcUSD.csv.gz'
        data = dataFromUrlGzCsv(dataUrl) #[:200]
        for row in data:
            pricePoint = row.split(',')
            time = pricePoint[0]
            price = pricePoint[1]
            volume = pricePoint[2]
            # fba.put('/btc/' + str(time), 'btc', price)
            fba.put('/btc/', str(time), price)
            # log.append({'time': time, 'price': price})
        return log

def lambda_handler(event, context):
    # code that will actually be run by AWS Lambda
    import urllib2
    # from firebase import firebase
    # import json
    # import logging
    # import pymysql
    # import requests

    # currently used functions
    def stripForTag(html, tag):
        # helper function which strips down the given html to just the content inside of a given tag
        openTag = '<' + tag
        if (html.find(openTag) < 0):
            print 'Tag ' + tag + ' not found'
            return ''
        closeTag = '</' + tag
        tagStart = html.find(openTag)
        tagEnd = html.find(closeTag)
        return html[html.find('>', tagStart) + 1:tagEnd]
    def getPriceHistorySourceFileList():
        # get list of data files from the source_url
        html = urllib2.urlopen(config['source_url']).read()
        innerHtml = stripForTag(html, 'pre')
        urls = [config['source_url'] + l[l.find('>') + 1 : l.find('<')] for l in innerHtml.split() if l[0:5] == 'href='][1:]
        config['source_file_list'] = urls
        log.append(config['source_file_list'][0])

    # maybe will use functions
    def getDataFromSource():
        # helper function to get JSON from a url
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
        return c
    def createTable(connection):
        connection.cursor.execute('CREATE TABLE BTC (time BIGINT NOT NULL, price_usd FLOAT, price_btc FLOAT, 24h_volume_usd BIGINT, market_cap_usd BIGINT, available_supply BIGINT, total_supply BIGINT, PRIMARY KEY (time))')
        pass
    def showARow(cursor):
        cursor.execute('select * from BTC')
        return cursor.fetchone()
    def showAllRows(cursor):
        cursor.execute('select * from BTC')
        return cursor.fetchall()
    def showTables(cursor):
        cursor.execute('SHOW TABLES')
        return cursor.fetchall()
    def insertRow(connection, data):
        # helper function to write data to RDS
        query = ("INSERT INTO BTC (time, price_usd, price_btc, 24h_volume_usd, market_cap_usd, available_supply, total_supply) VALUES (data.last_updated, data.price_usd, data.price_btc, data.24h_volume_usd, data.market_cap_usd, data.available_supply, data.total_supply)")
        try:
            connection.cursor.execute(query)
            connection.commit()
        except:
            logger.error('Error when inserting row')

    # main sequence
    getPriceHistorySourceFileList()
    return log

if __name__ == '__main__':
    # build, deploy and run function
    # this code will only run locally
    import os
    os.system('clear')
    print 'Building and deploying package to Lambda'
    if config['fresh_build']:
        os.system('ls | grep -v lambda_function.py | xargs rm -r')
    # if not os.path.exists('requests'):
    #   os.system('pip install requests -t .')
    # if not os.path.exists('firebase'):
    #   os.system('pip install python-firebase -t .')
    # if not os.path.exists('pymysql'):
    #   os.system('pip install pymysql -t .')
    os.system('zip -rq deployment-package.zip ./*')
    os.system('aws lambda update-function-code --function-name ' + config['function_name'] + ' --zip-file fileb://./deployment-package.zip')
    if config['clear_build']:
        os.system('ls | grep -v lambda_function.py | xargs rm -r')
    print 'Running function'
    os.system('aws lambda invoke --function-name ' + config['function_name'] + ' lambda_return.txt')
    os.system('clear')
    print 'Lambda return:'
    os.system('cat lambda_return.txt')
    os.system('rm lambda_return.txt')
    print
