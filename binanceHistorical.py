from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
import time

url = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2022-04.zip'
smallurl = 'https://data.binance.vision/data/spot/monthly/trades/BTCSTUSDT/BTCSTUSDT-trades-2022-06.zip'
smallerurl = 'https://data.binance.vision/data/spot/monthly/trades/1INCHUSDT/1INCHUSDT-trades-2022-07.zip'
url_525mb = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2022-03.zip'
url_1003mb = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2021-04.zip'
url_1gb = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2021-03.zip'
url_1point7gb = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2022-07.zip'
url = url_525mb


# resp = urlopen(url)
# zipfile = ZipFile(BytesIO(resp.read()))

# filename=zipfile.namelist()[0]
# print(filename)


# START GOOD CODE
# resp = urlopen(url)
# zipfile = ZipFile(BytesIO(resp.read()))
# filename=zipfile.namelist()[0]
# print(filename)
# fdf = pd.DataFrame()
# with open(f'./{filename}.txt', 'a') as f:
#     for line in zipfile.open(filename).readlines():
#         decoded=line.decode('utf-8')
#             # Uncomment to write data to file:
#         # f.write(decoded)
#         splitline=decoded.split(',')
#         print('SPLITLINE: ',splitline)
#         c=0
#             # Format for 'trades' data:
#         # trade Id	price	qty	quoteQty	time	isBuyerMaker	isBestMatch
#         df = pd.DataFrame({'tradeID':[splitline[0]], 'price':splitline[1],'qty':[splitline[2]],'quoteQty':[splitline[3]],'time':[splitline[4]],'isBuyerMaker':[splitline[5]],'isBestMatch':[splitline[6]]})
#         fdf=pd.concat([fdf,df])

# print('----------DATAFRAME CREATION FINISHED----------')


def binance_save_to_file(url):
    resp = urlopen(url)
    zipfile = ZipFile(BytesIO(resp.read()))
    filename = zipfile.namelist()[0]
    with open(f'./binance_{filename}', 'a') as f:
        # Format for 'trades' data:
        # trade Id	price	qty	quoteQty	time	isBuyerMaker	isBestMatch
        f.write('tradeID, price, qty, quoteQty, time, isBuyerMaker, isBestMatch')
        for line in zipfile.open(filename).readlines():
            decoded = line.decode('utf-8')
            # Uncomment to write data to file:
            f.write(decoded)
    print(f'binance_{filename} has been written')


def binance_to_dataframe(url):
    resp = urlopen(url)
    zipfile = ZipFile(BytesIO(resp.read()))
    filename = zipfile.namelist()[0]
    print(filename)
    fdf = pd.DataFrame()
    counter=0
    for line in zipfile.open(filename).readlines():
        decoded = line.decode('utf-8')
        splitline = decoded.split(',')
        # Format for 'trades' data:
        # trade Id	price	qty	quoteQty	time	isBuyerMaker	isBestMatch
        df = pd.DataFrame(
            {'tradeID': [splitline[0]], 'price': splitline[1], 'qty': [splitline[2]], 'quoteQty': [splitline[3]],
             'time': [splitline[4]], 'isBuyerMaker': [splitline[5]], 'isBestMatch': [splitline[6]]})
        fdf = pd.concat([fdf, df])
        if(counter%10000==0):
            print(fdf)
        counter+=1
    print(f'{filename} has been written to a dataframe.')
    return fdf


def binance_to_dataframe_faster(url):
    resp = urlopen(url)
    zipfile = ZipFile(BytesIO(resp.read()))
    filename = zipfile.namelist()[0]
    print(filename)
    # fdf = pd.DataFrame()
    counter=0
    dictList=[]
    # allDict={}
    for line in zipfile.open(filename).readlines():
        decoded = line.decode('utf-8')
        splitline = decoded.split(',')
        # Format for 'trades' data:
        # trade Id	price	qty	quoteQty	time	isBuyerMaker	isBestMatch
        dict = {'tradeID': [splitline[0]], 'price': splitline[1], 'qty': [splitline[2]], 'quoteQty': [splitline[3]],
             'time': [splitline[4]], 'isBuyerMaker': [splitline[5]], 'isBestMatch': [splitline[6]]}
        dictList.append(dict)
        # df = pd.DataFrame(
        #     {'tradeID': [splitline[0]], 'price': splitline[1], 'qty': [splitline[2]], 'quoteQty': [splitline[3]],
        #      'time': [splitline[4]], 'isBuyerMaker': [splitline[5]], 'isBestMatch': [splitline[6]]})
        # fdf = pd.concat([fdf, df])
        if(counter%10000==0):
            print(dictList[-1])
            print()
        counter+=1
    fdf = pd.DataFrame().from_records(dictList)
    print(f'{filename} has been written to a dataframe.')
    return fdf


s = time.perf_counter()
dfRes = binance_to_dataframe_faster(url)
e = time.perf_counter()
print(f"binance_to_dataframe_faster time: {e - s:0.4f} seconds")

# start = time.perf_counter()
# dfRes = binance_to_dataframe(url)
# end = time.perf_counter()
# print(f"binance_to_dataframe time: {end - start:0.4f} seconds")

start1 = time.perf_counter()
dfpricemean = dfRes['price'].mean()
end1 = time.perf_counter()
print(f"mean of df price time: {end1 - start1:0.4f} seconds")
print('MEAN : ', dfpricemean)

# binance_save_to_file(url)
# binance_to_dataframe(url)
print('ALL FINISHED')
