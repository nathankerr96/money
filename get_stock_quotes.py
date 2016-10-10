
import pandas
import pandas.io.data
import datetime
import urllib2
import csv

YAHOO_TODAY="http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=sd1ohgl1vl1"

def get_quote_today(symbol):
    response = urllib2.urlopen(YAHOO_TODAY % symbol)
    reader = csv.reader(response, delimiter=",", quotechar='"')
    for row in reader:
        if row[0] == symbol:
            return row

## main ##
symbol = "TSLA"

history = pandas.io.data.DataReader(symbol, "yahoo", start="2014/1/1")
print history.tail(2)

today = datetime.date.today()
df = pandas.DataFrame(index=pandas.DatetimeIndex(start=today, end=today, freq="D"),
                      columns=["Open", "High", "Low", "Close", "Volume", "Adj Close"],
                      dtype=float)

row = get_quote_today(symbol)
df.ix[0] = map(float, row[2:])

history = history.append(df)

print "today is %s" % today
print history.tail(2)
