Pulls from stocks_list.txt

Expects folder called "data" where it saves all data in json format

Looks like google blocks around 2100 requests

Data is in the following form:
  <stock_name>.txt:
    {timestamp} (signifies start minute of market opening begins with a, will figure out how to convert to a date)
      {0-390} (signifies what minute within the day)
        [open, high, low, close, volume] (in this order, no key value so use num in list)
