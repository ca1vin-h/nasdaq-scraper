# Nasdaq trade scraper
#    use ticker as argument when run
#    Example command line argument:
#        python3 NasdaqScraper.py aapl brk-b tsla
#
#    Scrape trade data from Nasdaq api as csv files
#    Trade data contains all trades made on most recent trading day
#    Script cannot be run during NYSE trading hours

#todo: add logic to remove responses that do not contain data

'''
Source Website:
https://www.nasdaq.com/market-activity/stocks/AAPL/latest-real-time-trades

Example api curl requests

curl 'https://api.nasdaq.com/api/quote/AAPL/realtime-trades?&limit=20&fromTime=9:30' \
  -H 'user-agent: Samsung SmartFridgeKit V420.69' \
  --compressed

curl 'https://api.nasdaq.com/api/quote/AAPL/realtime-trades?&limit=9&offset=0&fromTime=09:30' \
  -H 'user-agent: Samsung SmartFridgeKit V420.69' \
  --compressed

Ticker search
curl 'https://api.nasdaq.com/api/autocomplete/slookup/10?search=aapl' \
  -H 'user-agent: Samsung SmartFridgeKit V420.69' \
  --compressed

'''

from datetime import datetime
from pytz import timezone
import os
import pandas as pd
import requests
import time
import trading_calendars as tc #outdated, does not include juneteenth, https://github.com/rsheftel/pandas_market_calendars
import csv

#output folder
OUT_DIR = "outtest/"


TIME_BLOCKS = ["15:30", "15:00", "14:30", "14:00", "13:30", "13:00", "12:30", "12:00", "11:30", "11:00", "10:30", "10:00", "09:30"]
REQUEST_HEADER = {'user-agent': 'Samsung SmartFridgeKit V420.69'}
MAX_ROWS = 9999
NETWORK_TIMEOUT = 10

#https://api.nasdaq.com/api/quote/AAPL/realtime-trades?&limit=9&offset=0&fromTime=09:30
URL_DOMAIN = "https://api.nasdaq.com/api/quote/"
URL_DIR = "/realtime-trades?&limit="
URL_OFFSET = "&offset="
URL_TIME = "&fromTime="


#Create Exceptions
class ActiveTradingHoursException(Exception):
    "Program cannot be run during NYSE trading hours"
    pass

class IncorrectRecordSumException(Exception):
    "Actual number of records does not equal expected number of records (data is missing or duplicated)"
    pass

class InvalidTickerException(Exception):
    "Ticker does not exist on Nasdaq as a stock or etf"
    pass

class InvalidRequestException(Exception):
    "URL incorrect, api may have been moved or require additional headers"
    pass



def _getDataFromTimeBlock(ticker, timeBlock, csv_writer):
    offset = 0
    totalRecords = 1
    n = 1
    while totalRecords > offset:
        url = URL_DOMAIN + ticker + URL_DIR + str(MAX_ROWS) + URL_OFFSET + str(offset) + URL_TIME + timeBlock
        response = requests.get(url, headers=REQUEST_HEADER, timeout=NETWORK_TIMEOUT)
        jsonresponse = response.json()

        responseCode = jsonresponse['status']['rCode']
        if responseCode != 200:
            raise InvalidRequestException
        
        if n == 1:
            totalRecords = jsonresponse['data']['totalRecords']
        if totalRecords == 0:
            continue

        for data in jsonresponse['data']['rows']:
            data['nlsShareVolume'] = data['nlsShareVolume'].replace(",","")
            data['nlsPrice'] = data['nlsPrice'].replace("$","")
            data['nlsPrice'] = data['nlsPrice'].replace(" ","")
            data['nlsTime'] = data['nlsTime'].replace(" ","")
            csv_writer.writerow(data.values())

        offset = MAX_ROWS * n
        n = n + 1
    return totalRecords

#returns previous close
#does not work
def getTradeDate():
    return "date"
    xnys = tc.get_calendar("XNYS")
    currentTimeStamp = pd.Timestamp(datetime.today())
    if xnys.is_session(currentTimeStamp):
        print("NYSE trading hours end at " + str(xnys.next_close(currentTimeStamp).tz_convert(tz='US/Eastern')))
        raise ActiveTradingHoursException
    return str(xnys.previous_close(currentTimeStamp))[:10]


def getTicker(ticker, outFolder):
    ticker = ticker.upper()
    tradeDate = getTradeDate()
    sumTotalRecords = 0
    if not os.path.exists(outFolder + ticker + "/"):
        os.makedirs(outFolder + ticker + "/")
    outFile = open(outFolder + ticker + "/" + tradeDate + ".csv", 'w+')
    csv_writer = csv.writer(outFile)

    for timeBlock in TIME_BLOCKS:
        sumTotalRecords = sumTotalRecords + _getDataFromTimeBlock(ticker, timeBlock, csv_writer)

    outFile.seek(0)

    if int(sumTotalRecords) != int(len(outFile.readlines())):
        outFile.close()
        raise IncorrectRecordSumException
    outFile.close()

# returns ticker type or throws exception InvalidTickerException
def checkIfTicker(query):
    query = query.upper()
    response = requests.get("https://api.nasdaq.com/api/autocomplete/slookup/5?search=" + query, headers=REQUEST_HEADER, timeout=NETWORK_TIMEOUT)
    jsonresponse = response.json()
    if jsonresponse['data']:
        for result in jsonresponse['data']:
            if result['symbol'] == query:
                if (result['asset'] == "STOCKS") or (result['asset'] == 'ETF'):
                    return result['asset']
    raise InvalidTickerException


def scrape(ticker):
    ticker = ticker.upper()
    print(ticker)

    flag = True
    while flag:
        try:
            print(ticker + " was found on Nasdaq and is a " + checkIfTicker(ticker))
            getTicker(ticker, OUT_DIR)
            print("Finished " + ticker)
            print()
            flag = False

        except InvalidTickerException:
            print("Ticker \"" + ticker + "\" does not exist on Nasdaq as a stock or ETF")
            print("Please ensure that the Stock Ticker Symbol is used with period ")
            print('Ex: "BRK.B", "AAPL", "AMD"')
            print("Skipping \"" + ticker + "\"")
            flag = False

        except ActiveTradingHoursException:
            print("Program cannot be run during NYSE trading hours (Ticker: " + ticker + ")")
            time.sleep(60)
            flag = True

        except IncorrectRecordSumException:
            print("Actual number of records does not equal expected number of records (data is missing or duplicated) (Ticker: " + ticker + ")")
            flag = True

        except InvalidRequestException:
            print("Unexpected response from nasdaq server, URL or headers may be invalid.")
            time.sleep(60)
            flag = True

        except requests.exceptions.ReadTimeout:
            print("The server did not send any data in the allotted amount of time (Ticker: " + ticker + ")")
            flag = True

        except requests.exceptions.ConnectionError:
            print("A connection error occurred, sleeping 5 seconds (Ticker: " + ticker + ")")
            time.sleep(5)
            flag = True

top100Tickers = ["LDSF", "BRK.A", "BRK.B"]

"""
top100Tickers = ["gasdfbsgbngbghnghghge","AAPL","MSFT","GOOG","AMZN","BRK.A","BRK.B","NVDA","META","TSLA","V","UNH","XOM","JNJ","WMT","LLY","JPM","MA","PG","CVX","MRK","HD","KO","PEP","ORCL","ABBV","AVGO","COST","BAC","PFE","MCD",
                 "TMO","CRM","ABT","NKE","CSCO","DIS","DHR",
                 "TMUS","CMCSA","VZ","ADBE","AMD","NEE","UPS","PM","NFLX","TXN","WFC","BMY","RTX","MS","HON","INTC","AMGN","LOW","COP",
                 "SBUX","UNP","T","INTU","BA","QCOM","PLD","LMT","SPGI","AXP","DE","GE","CAT","IBM","ELV","SYK","GS","MDLZ","ISRG",
                 "GILD","BKNG","AMAT","BLK","ADI","TJX","C","AMT","NOW","VRTX","CVS","MMC","ADP","SCHW","ZTS","MO","REGN","SO","ABNB",
                 "PGR","CI","UBER","BSX","DUK","HCA","PYPL"]
"""
for ticker in top100Tickers:
    scrape(ticker)