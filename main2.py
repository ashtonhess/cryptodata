from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
url = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2022-04.zip'
smallurl = 'https://data.binance.vision/data/spot/monthly/trades/BTCSTUSDT/BTCSTUSDT-trades-2022-06.zip'
# url=smallurl;
# resp = urlopen(url)
# zipfile = ZipFile(BytesIO(resp.read()))

# filename=zipfile.namelist()[0]
# print(filename)

resp = urlopen(url)
zipfile = ZipFile(BytesIO(resp.read()))
filename=zipfile.namelist()[0]
print(filename)
with open(f'./data/{filename}.txt', 'a') as f:
    for line in zipfile.open(filename).readlines():
        f.write(line.decode('utf-8'))
        # print('l')






# Final version running on remote
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
url = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2022-04.zip'

# resp = urlopen(url)
# zipfile = ZipFile(BytesIO(resp.read()))

# filename=zipfile.namelist()[0]
# print(filename)

resp = urlopen(url)
zipfile = ZipFile(BytesIO(resp.read()))
filename=zipfile.namelist()[0]
print(filename)
with open(f'./{filename}.txt', 'a') as f:
    for line in zipfile.open(filename).readlines():
        decoded=line.decode('utf-8')
        f.write(decoded)
        # print(decoded)

import os
file_size = os.path.getsize(f'./{filename}')
print("File Size is :", file_size, "bytes")
print("File Size is :", file_size/1000000000, "bytes")
