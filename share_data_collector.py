from db_connector import engine
from config import SHARE_TABLE_NAME, STOCKS
from creds import ALPACA_API_KEY, ALPACA_SECRET_KEY, FMP_API_KEY, SOCKET_URL
import pandas as pd
import datetime
import json
import websocket
import ssl
import threading
from forex_python.converter import CurrencyRates
import requests
import time

class MessageHandler():

    def __init__(self):
        self.last_hour = -1
        self.last_close = -1
        self.api_index = 0
        self.market_cap = self.get_market_cap()

    def on_open(self, ws):
        print("opened")
        auth_data = {
            "action": "auth",
            "key": ALPACA_API_KEY,
            "secret": ALPACA_SECRET_KEY
        }
        ws.send(json.dumps(auth_data))
        listen_message = {"action": "subscribe", "bars": STOCKS}
        ws.send(json.dumps(listen_message))


    def handle_socket_message(self, ws, message):
        msg = json.loads(message)
        print(msg)
        if msg[0]["T"] == "b":
            print("received data")
            df = pd.DataFrame(msg)
            df = df.loc[:, ['t', 'S', 'o', 'c', 'h', 'l', 'v', 'n']]
            df.columns = ['starttime', 'symbol', 'open',
                          'close', 'high', 'low', 'basevolume', 'tradecount']
            df["market_cap"] = 0
            
            symbol = df.loc[0, "symbol"]
            
            # Data Preperation
            df.open = df.open.astype(float)
            df.close = df.close.astype(float)
            df.high = df.high.astype(float)
            df.low = df.low.astype(float)
            df.market_cap = df.market_cap.astype(float)
            df.starttime = pd.to_datetime(df.starttime).dt.tz_localize(None)

            #Update market cap
            if self.last_hour != datetime.datetime.now().hour:
                print(self.get_market_cap())
                self.market_cap = self.get_market_cap()
                self.last_hour = datetime.datetime.now().hour
            else:
                pct_change = 1
                if self.market_cap.loc[self.market_cap["symbol"] == symbol, "last_close"].values[0] != -1:
                   pct_change = df.loc[0, "close"] / self.market_cap.loc[self.market_cap["symbol"] == symbol, "last_close"].values[0]
                   self.market_cap.loc[self.market_cap["symbol"] == symbol, "market_cap"] *= pct_change
                print(pct_change)

            df["market_cap"] = self.market_cap.loc[self.market_cap["symbol"] == symbol, "market_cap"].values[0]
            self.market_cap.loc[self.market_cap["symbol"] == symbol, "last_close"] = df.loc[0, "close"]
            
            print(df)
            # Convert to Euros
            conversion_rate = CurrencyRates().get_rate('USD', 'EUR')
            df.loc[:, ["open", "high", "low", "close", "market_cap"]] *= conversion_rate
            # Write to data base
            try:
                df.to_sql(SHARE_TABLE_NAME, engine, if_exists='append', index=False)
            except:
                print("DATABASE UNAVAILABLE SKIPPING WRITE")
                
            print(df)


    def on_close(self, ws):
        print("closed connection")

    def get_market_cap(self):
        # start any sockets here
        mp_list = []
        for stock in STOCKS:
            url = (f"https://financialmodelingprep.com/api/v3/market-capitalization/{stock}?apikey={FMP_API_KEY[self.api_index]}")
            r = requests.get(url = url)
            # If api call fails try different API Keys
            while r.status_code != 200 and self.api_index < (len(FMP_API_KEY) - 1) :
                print("FMP API limit exceeded - switching keys")
                self.api_index += 1
                url = (f"https://financialmodelingprep.com/api/v3/market-capitalization/{stock}?apikey={FMP_API_KEY[self.api_index]}")
                r = requests.get(url = url)
            # If no API request was succesful write a default message
            if r.status_code == 200:
                mp_list.append(r.json()[0])
            else:
                print("skipping marketcap no API data available")
                default = {'symbol': stock, 'date': '1999-01-01', 'marketCap': 0}
                mp_list.append(default)
        self.api_index = 0

        df = pd.DataFrame(mp_list)
        df = df.loc[:, ["symbol", "marketCap"]]
        df.columns = ["symbol", "market_cap"]
        df["last_close"] = -1
        return df


class LiveDataCollector():

    def __init__(self):
        self.run = True
        self.thread = threading.Thread(name='ws_endless_loop', target=self.start_websocket)

    def start_websocket(self):
        handler = MessageHandler()
        while True:
            ws = websocket.WebSocketApp(SOCKET_URL, on_open=handler.on_open,
                            on_message=handler.handle_socket_message, on_close=handler.on_close)
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            time.sleep(10)
            print("attemting reconnect")

    def start(self):
        self.thread.start()
        print("Thread started")

    def stop(self):
        if self.thread is not None:
            self.thread.join()

if __name__ == "__main__":
    ldc = LiveDataCollector()
    ldc.start()
    print("arrived here")