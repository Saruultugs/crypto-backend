from firebase import firebase
import json
import logging
import pymysql
import requests
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#rds settings
rds_host = "crypto-data.crapipl8g1ks.us-east-2.rds.amazonaws.com"
name = "shiraz"
password = "crypto_rds"
db_name = "crypto_data"
table_name = "coinmarketcap"

#coinmarketcap configuration
NUM_CURRENCIES = 15      # Pull data of only top ## of currencies
URL = "https://api.coinmarketcap.com/v1/ticker/?limit=" + str(NUM_CURRENCIES)

def getDataFromURL():
    # When given a URL, the content of that page will be returned as JSON entries in a list.
    page = requests.get(URL)
    return json.loads(page.content)

def openMySQLConnection():
    try:
        connection = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()
    logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
    return connection

def writeToRDS(connection, cursor, data):
    log = ['Writing currency price data to RDS']
    timestamp = data[0]["last_updated"]
    for currency in data:
        query = ("REPLACE INTO coinmarketcap (time, id, name, symbol, rank, price_usd,"
                 "price_btc, 24h_volume_usd, market_cap_usd, available_supply,"
                 "total_supply, percent_change_1h, percent_change_24h, percent_change_7d)"
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        try:
            cursor.execute(query, (
                timestamp, 
                currency["id"], 
                currency["name"],
                currency["symbol"], 
                currency["rank"], 
                currency["price_usd"], 
                currency["price_btc"],
                currency["24h_volume_usd"], 
                currency["market_cap_usd"], 
                currency["available_supply"],
                currency["total_supply"], 
                currency["percent_change_1h"], 
                currency["percent_change_24h"],
                currency["percent_change_7d"]))
            connection.commit()
            log.append("Row successfully added for {0}: {1} at {2}".format(currency["id"], currency["price_usd"], currency["last_updated"]))
        except Exception as e:
            logger.error("ERROR: Could not insert row. {0}".format(e))
            log.append("ERROR: Could not insert row. {0}".format(e))
    return log

def getFirebaseApplication():
    return firebase.FirebaseApplication('https://crypto-trading-953aa.firebaseio.com/', None)

def writePricesToFirebase(fba, data):
    # TODO: This function needs to be updated to include all parameters listed in insertRow()
    log = ['Writing currency price data to Firebase']
    dataUrl = 'https://api.bitcoincharts.com/v1/csv/localbtcUSD.csv.gz' 
    fba.put('/price/' + str(data["last_updated"]), 'usd', 1)
    fba.put('/price/' + str(data["last_updated"]), 'btc', data["price_usd"])
    fba.put('/price/' + str(data["last_updated"]), '24h_volume_usd', data["24h_volume_usd"])
    fba.put('/price/' + str(data["last_updated"]), 'market_cap_usd', data["market_cap_usd"])
    fba.put('/price/' + str(data["last_updated"]), 'available_supply', data["available_supply"])
    fba.put('/price/' + str(data["last_updated"]), 'total_supply', data["total_supply"])
    fba.put('/price/' + str(data["last_updated"]), 'source', "coinmarketcap")
    log.append({'time': data["last_updated"], 'price': data["price_usd"]})
    return log

def lambda_handler(event, context):
    log = ['Starting main lambda tasks.']
    data = getDataFromURL()
    connection = openMySQLConnection()
    cursor = connection.cursor()
    log.append(writeToRDS(connection, cursor, data))
    #fba = getFirebaseApplication()
    #log.append(writePricesToFirebase(fba, data[0]))
    return log