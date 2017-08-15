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

def main(fba):
    log = ['Running main Lambda tasks']
    # log.append(updateStatus(fba))
    log.append(writePrices(fba))
    return log

def lambda_handler(event, context):
    fba = getFirebaseApplication()
    return main(fba)