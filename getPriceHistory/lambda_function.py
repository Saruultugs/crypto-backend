# --function-name getPriceHistory

# set global variables (for both build and runtime)
config = {
    'function_name': 'getPriceHistory',
    'source_url': 'https://api.bitcoincharts.com/v1/csv/',
    'db': 'currency_prices',
    'url': 'https://api.coinmarketcap.com/v1/ticker/?limit=1',
    'firebase_url': 'https://crypto-trading-953aa.firebaseio.com/',
    'fresh_build': True,
    'clear_build': True
}
rds = {
    'host': 'crypto-data.crapipl8g1ks.us-east-2.rds.amazonaws.com',
    'user': 'shiraz',
    'passwd': 'crypto_rds',
    'db': 'crypto_data',
    'connect_timeout': 5,
    'table_name': 'coinmarketcap'
}
log = ['Running ' + config['function_name']]

def lambda_handler(event, context):
    import os
    import gzip
    import json
    import boto3
    import pymysql
    import urllib2
    import StringIO
    from firebase import firebase
    # import requests

    def openRdsConnection():
        try:
            rds['cursor'] = pymysql.connect(host=rds['host'], user=rds['user'], passwd=rds['passwd'], db=rds['db'], connect_timeout=rds['connect_timeout']).cursor()
            log.append("Connected to RDS")
            return rds['cursor']
        except:
            log.append("Error connecting to RDS")

    def runQuery(q):
        rds['cursor'].execute(q)
        return rds['cursor'].fetchall()
        # return rds['cursor'].fetchone()

    def getTables():
        return [x[0] for x in runQuery('show tables')]
        pass

    def tableExists(t):
        return t in getTables()
        pass

    def createTable(t):
        q = 'create table ' + t
        q =+ ' (time bigint not null, price float, volume float, primary key (time))'
        return runQuery(q)

    def getRowCount(t):
        return runQuery('select count(0) from ' + t)[0][0]
        pass

    def getSample(t, n=5):
        return runQuery('select * from %s limit %d' % (t, n))
        pass

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

    def getSourceFiles():
        # get list of data files from the source_url
        html = urllib2.urlopen(config['source_url']).read()
        innerHtml = stripForTag(html, 'pre')
        urls = [config['source_url'] + l[l.find('>') + 1 : l.find('<')] for l in innerHtml.split() if l[0:5] == 'href='][1:]
        config['source_file_dicts'] = [ { 'name': url[url.rfind('/')+1:-7], 'url': url } for url in urls]
        log.append('Source files: ' + str(len(config['source_file_dicts'])))
        return config['source_file_dicts']

    def loadSourceData(url):
        compressedFile = StringIO.StringIO()
        compressedFile.write(urllib2.urlopen(url).read())
        compressedFile.seek(0)
        decompressedFile = gzip.GzipFile(fileobj=compressedFile, mode='rb')
        config['current_data'] = [d.strip('\n') for d in decompressedFile.readlines()]
        return config['current_data']

    def getDataShape(d):
        rows = len(d)
        sample = [len(r.split(',')) for r in d[:1000:100]]
        cols = sum(sample) / len(sample)
        for i, r in enumerate(d):
            if (len(r.split(',')) != cols):
                log.append('WARNING: Row ' + i + ' does not conform')
        return rows, cols

    def writeRecord(t, r):
        q = 'insert into ' + t + ' '
        q += '(time, price, volume) '
        q += 'values (%s, %s, %s)' % (r[0], r[1], r[2])
        return runQuery(q)

    def main():
        c = openRdsConnection()
        source_files = getSourceFiles()
        for f in source_files[192:193]:

            # get table
            table = f['name'].lower()
            if not tableExists(table): createTable(table)
            rows_start = getRowCount(table)

            # get data
            data = loadSourceData(f['url'])
            rows_new, cols_new = getDataShape(data)

            # write records
            for d in data[0:1000]:
                writeRecord(table, d.split(','))

            # review
            rows_end = getRowCount(table)
            log.append('%s: %d+%d-%d=%d' % (table, rows_start, rows_new, rows_end, rows_start+rows_new-rows_end)) # (table: start+new-written=diff) the last number should equal zero

    def test():
        if not 'cursor' in rds:
            c = openRdsConnection()
        log.append(getRowCount('localbtcusd'))
        log.append(getSample('localbtcusd'))

    def runFirebase():
        def getFirebase():
            return firebase.FirebaseApplication(config['firebase_url'], None)
            pass
        def getFbKeys(b=''):
            url = config['firebase_url'] + b + '.json?shallow=true&print=pretty'
            try:
                keys = json.loads(urllib2.urlopen(url).read()).keys()
            except:
                keys = []
            return keys
        def tests():
            def firebaseAccessible():
                return False
            def expectedRootKeysPresent():
                return False
            def noUnexpectedRootKeysPresent():
                return False
            def expectedRowCountsPresent():
                return False
            def lastExpectedUpdatePresent():
                return False
        fb = getFirebase()
        root_keys = getFbKeys()
        log.append(root_keys)
        for k in root_keys:
            log.append('%s: %d' % (k, len(getFbKeys(k))))

    def other():
        pass
        # log.append('%s: %s + %s = %s' % (table, str(rows_start), str(rows_new), str(rows_start+rows_new)))
        # url = config['source_file_list'][0]
        # log.append(url[url.rfind('/')+1:-7])
        # singleFile = [f for f in config['source_file_list'] if 'localbtcUSD' in f][0]
        # rds['cursor'].execute('select count(0) from coinmarketcap')
        # return fba.get('/', None)
        # return fba.post('/currency/', {})
        # fba.put('/btc/', str(time), price)
        # return fba.delete(key, None)
        # query = ("INSERT INTO BTC (time, price_usd, price_btc, 24h_volume_usd, market_cap_usd, available_supply, total_supply) VALUES (data.last_updated, data.price_usd, data.price_btc, data.24h_volume_usd, data.market_cap_usd, data.available_supply, data.total_supply)")
        # connection.cursor.execute(query)
        # connection.commit()

    # main()
    # test()
    runFirebase()
    return log

if __name__ == '__main__':
    import os
    os.system('clear')
    print 'Building and deploying package to Lambda'
    if config['fresh_build']:
        os.system('ls | grep -v lambda_function.py | xargs rm -r')
    if not os.path.exists('pymysql'):
        os.system('pip install pymysql -t .')
    # if not os.path.exists('numpy'):
    #     os.system('pip install "numpy==1.5.1" -t .')
    if not os.path.exists('requests'):
      os.system('pip install requests -t .')
    if not os.path.exists('firebase'):
      os.system('pip install python-firebase -t .')
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
