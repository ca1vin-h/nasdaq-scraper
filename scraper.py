#Nasdaq trade scraper
#Scrape trade data from Nasdaq api as csv files

import requests
from datetime import datetime
import os

# Script must be run between 16:00-23:59 on trading days to ensure date is accurate
# Date must be manually set if program is run over the weekend or on a non trading day
# The api does not return an accurate date
todaysDate = datetime.today().strftime('%Y-%m-%d')
#todaysDate = "2023-06-09"



'''
Example api curl requests

curl 'https://api.nasdaq.com/api/quote/AAPL/realtime-trades?&limit=20&fromTime=9:30' \
  -H 'user-agent: Samsung SmartFridgeKit V420.69' \
  --compressed

curl 'https://api.nasdaq.com/api/quote/AAPL/realtime-trades?&limit=9&offset=60&fromTime=09:30' \
  -H 'user-agent: Samsung SmartFridgeKit V420.69' \
  --compressed

  
Website:
https://www.nasdaq.com/market-activity/stocks/AAPL/latest-real-time-trades

  '''


timeBlocks = ["15:30", "15:00", "14:30", "14:00", "13:30", "13:00", "12:30", "12:00", "11:30", "11:00", "10:30", "10:00", "09:30"]

top100Tickers = ["AAPL","MSFT","GOOG","AMZN","BRK%25sl%25A","BRK%25sl%25B","NVDA","META","TSLA","V","UNH","XOM","JNJ","WMT","LLY","JPM","MA","PG",
                 "CVX","MRK","HD","KO","PEP","ORCL","ABBV","AVGO","COST","BAC","PFE","MCD","TMO","CRM","ABT","NKE","CSCO","DIS","DHR",
                 "TMUS","CMCSA","VZ","ADBE","AMD","NEE","UPS","PM","NFLX","TXN","WFC","BMY","RTX","MS","HON","INTC","AMGN","LOW","COP",
                 "SBUX","UNP","T","INTU","BA","QCOM","PLD","LMT","SPGI","AXP","DE","GE","CAT","IBM","ELV","SYK","GS","MDLZ","ISRG",
                 "GILD","BKNG","AMAT","BLK","ADI","TJX","C","AMT","NOW","VRTX","CVS","MMC","ADP","SCHW","ZTS","MO","REGN","SO","ABNB",
                 "PGR","CI","UBER","BSX","DUK","HCA","PYPL"]


headers = {'user-agent': 'Samsung SmartFridgeKit V420.69'}
url_domain = "https://api.nasdaq.com/api/quote/"
url_dir = "/realtime-trades?&limit="
url_offset = "&offset="
url_time = "&fromTime="
MAX_ROWS = 9999



def makeURL(ticker, offset, timeBlock):
    return url_domain + ticker + url_dir + str(MAX_ROWS) + url_offset + str(offset) + url_time + timeBlock

def cleanData(s):
    s = s.replace("\",\"", "#")
    s = s.replace(",", "")
    s = s.replace("#", ",")
    s = s.replace("\"", "")
    s = s.replace("$", "")
    s = s.replace(" ", "")
    return s

def cleanTicker(ticker):
    return ticker.replace("%25sl%25", "-")

def getTotalRecords(ticker, s):
    s = s[(28 + len(cleanTicker(ticker))):]
    return int(s[:s.find("o")])

def toCSV(s):
    s = s.replace("e}rows:[{", "^")
    s = s.replace("}]topTable:", "@")
    s = s.replace("}{", "\n")
    s = s.replace("nlsTime:", "")
    s = s.replace("nlsPrice:", "")
    s = s.replace("nlsShareVolume:", "")
    s = s[(s.find("^")+1):]
    s = s[:(s.find("@"))]
    return s

def getDataFromTimeBlock(ticker, timeBlock, outFile):
    offset = 0
    totalRecords = 1
    n = 1
    while totalRecords > offset:
        response = requests.get(makeURL(ticker, offset, timeBlock), headers=headers, timeout=60)
        responseString = cleanData(str(response.content, 'utf-8'))
        if n == 1:
            totalRecords = getTotalRecords(ticker, responseString)
        outFile.write(toCSV(responseString) + "\n")
        offset = MAX_ROWS * n
        n = n + 1

def makePath(ticker):
    if not os.path.exists("out/" + ticker + "/"):
        os.makedirs("out/" + ticker + "/")

def getTicker(ticker, todaysDate):
    makePath(cleanTicker(ticker))
    outFile = open("out/" + cleanTicker(ticker) + "/" + todaysDate + ".csv", 'w')

    for timeBlock in timeBlocks:
        getDataFromTimeBlock(ticker, timeBlock, outFile)
    
    outFile.close()


for ticker in top100Tickers:
    print(ticker)
    getTicker(ticker, todaysDate)
