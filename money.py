import urllib2
import json


quote_list = open("./stocks_list.txt")
QUOTES = quote_list.read().split()

for quote in QUOTES:
    try:
        store_file = open("./data/" + quote + ".json")
    except IOError:
        store_file = open("./data/" + quote + ".json", "w")
        store_file.close()
        store_file = open("./data/" + quote + ".json")

    current_data = store_file.read()
    store_file.close()

    if current_data:
        current_data = json.loads(current_data)    
    else:
        current_data = {}
        
    print quote


    raw_data = urllib2.urlopen('https://www.google.com/finance/getprices?i=60&p=10d&f=d,o,h,l,c,v&df=cpct&q='+quote).read().split()
    if raw_data[0] == "EXCHANGE%3DUNKNOWN+EXCHANGE":
        print "Could not find quote: " + quote
        continue

    for i in range(0,7):
        try:
            raw_data.pop(0)
        except IndexError:
            print "No Data Found!"
            continue

    for row in raw_data:
        tick = row.split(',')

        if tick[0][0] == 'a':
            current_time_stamp = tick[0]
            if current_time_stamp in current_data:
                continue
            current_data[current_time_stamp] = {}
            current_data[current_time_stamp][0] = [tick[1],tick[2],tick[3],tick[4],tick[5]]
            continue

        if tick[0] in current_data[current_time_stamp]:
            continue
        else:
            current_data[current_time_stamp][tick[0]] = [tick[1],tick[2],tick[3],tick[4],tick[5]]
        

    store_file = open("./data/" + quote + ".json", "w")
    store_file.write(json.dumps(current_data))
    store_file.close() 
