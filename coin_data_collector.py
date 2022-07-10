from db_connector import engine
from binance import ThreadedWebsocketManager
from coinmarketcapapi import CoinMarketCapAPI
from config import COINS, CRYPTO_TABLE_NAME
from creds import MARKET_API_KEY, BINANCE_API_KEY, BINANCE_SECRET_KEY
import pandas as pd
import datetime


class MessageHandler():

    def __init__(self):
        self.last_hour = -1
        self.last_close = -1
        self.market_cap = self.get_market_cap()
        

    def handle_socket_message(self, msg):
        # Check if full minute passed
        if msg['k']['x']:
            df = pd.DataFrame([msg['k']])
            df = df.loc[:, ['t', 's', 'o', 'c', 'h', 'l', 'v', 'q']]
            df.columns = ['starttime', 'symbol', 'open',
                          'close', 'high', 'low', 'basevolume', 'quotevolume']
            df["market_cap"] = 0
            symbol = df.loc[0, "symbol"]

            # Data Preperation
            df.open = df.open.astype(float)
            df.close = df.close.astype(float)
            df.high = df.high.astype(float)
            df.low = df.low.astype(float)
            df.market_cap = df.market_cap.astype(float)
            df.starttime = pd.to_datetime(df.starttime, unit="ms")

            print("#############MARKET_CAP_TABLE#############")
            print(self.market_cap)
            # Update market cap
            if self.last_hour != datetime.datetime.now().hour:
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

            # Write to data base
            try:
                df.to_sql(CRYPTO_TABLE_NAME, engine, if_exists='append', index=False)
            except:
                print("DATABASE UNAVAILABLE SKIPPING WRITE")        
            print("#############WRITTEN DATA#############")
            print(df)

    def get_market_cap(self):
        coins = ""
        for coin in COINS:
            coins += coin
            coins += ","
        coins = coins[:-1]
        coins

        cmc = CoinMarketCapAPI(api_key=MARKET_API_KEY)
        r = cmc.cryptocurrency_quotes_latest(symbol=coins, convert="EUR")
        df = pd.DataFrame(r.data)
        df = df.transpose()
        df = df.drop(["id", "name","slug","num_market_pairs","date_added","tags", "max_supply","circulating_supply","total_supply","is_active","platform","cmc_rank","is_fiat","last_updated"], axis=1)
        df["market_cap"] = df["quote"].apply(lambda x: x["EUR"]["market_cap"])
        df = df.drop("quote", axis=1)
        df["symbol"] = df["symbol"] + "EUR"
        df["last_close"] = -1

        df.market_cap = df.market_cap.astype(float)
        df.last_close = df.last_close.astype(float)
        return df
       


class LiveDataCollector():

    def __init__(self):
        self.run = True
        self.twm = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY, api_secret=BINANCE_SECRET_KEY)

    def start(self):
        handler = MessageHandler()
        self.twm.start()

        # start any sockets here
        for coin in COINS:
            symbol = f"{coin}EUR"
            self.twm.start_kline_socket(
                callback=handler.handle_socket_message, symbol=symbol)
            print(f"{symbol} socket started")

    def stop(self):
        self.twm.stop()


if __name__ == "__main__":
    ldc = LiveDataCollector()
    ldc.start()
