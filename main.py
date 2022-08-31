# import ssl
# import websocket
# def on_message(ws, message):
#     print(message)
#
# ws = websocket.WebSocketApp(
#     "wss://api.gemini.com/v1/marketdata/btcusd?top_of_book=true&bids=false",
#     on_message=on_message)
# ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

import asyncio
import websockets
import json


class LiveData():
    def __init__(self):
        self.tickers = ['btcusdt','ethusdt','bnbusdt','xrpusdt','adausdt','solusdt','dogeusdt','dotusdt','maticusdt','shibusdt']
        self.tasks = []

    async def symbol_stream_to_txt(self, sym):
        async with websockets.connect(f'wss://stream.binance.com:9443/ws/{sym}@trade') as websocket:
            with open(f'./data/{sym}.txt', 'a') as f:
                while True:
                    msg = await websocket.recv()
                    jMsg = json.loads(msg)
                    wMsg = str(jMsg['E'])+','+str(jMsg['p'])+'\n'
                    # x = str(jMsg['E'])
                    # y = str(jMsg['p'])
                    # wMsg = x+','+y+'\n'
                    # wMsg = msg + '\n'
                    f.write(wMsg)

    async def main(self):
        for ticker in self.tickers:
            task = asyncio.create_task(self.symbol_stream_to_txt(ticker))
            self.tasks.append(task)
        for task in self.tasks:
            await task
        # task1=asyncio.create_task(self.symbol_stream_to_txt('btcusdt'))
        # task2=asyncio.create_task(self.symbol_stream_to_txt('ethusdt'))
        # await task1
        # await task2

obj = LiveData()
asyncio.run(obj.main())

# obj = LiveData()
# asyncio.create_task(obj.symbol_stream_to_txt('btcusdt'))
# asyncio.create_task(obj.symbol_stream_to_txt('ethusdt'))


# obj.symbol_stream_to_txt('btcusdt')
# obj.symbol_stream_to_txt('ethusdt')

# asyncio.run(obj.symbol_stream_to_txt('btcusdt'))
# asyncio.run(obj.symbol_stream_to_txt('ethusdt'))

# asyncio.get_event_loop().create_task(obj.symbol_stream_to_txt('btcusdt'))
# asyncio.get_event_loop().create_task(obj.symbol_stream_to_txt('ethusdt'))
