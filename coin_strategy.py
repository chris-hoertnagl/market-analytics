from db_connector import engine, spot_engine
import pandas as pd
import numpy as np
from config import COIN_VIEW, COIN_TA_TABLE_NAME
import ta

class Util:
    def resample_to_dict(df_in):
        dfdict = dict(tuple(df_in.groupby("symbol")))
        df_in = None
        for key in dfdict:
            df = dfdict[key]
            df = df.sort_values("starttime")

            df = df.resample('1H', on="starttime").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "quotevolume": "sum",
                "symbol": "last"
            })

            dfdict[key] = df
        return dfdict
    
    def load_data(view_name, engine):
        df_ta = pd.read_sql(f"select * from \"{view_name}\"", engine.connect())
        df_ta.quotevolume = df_ta.quotevolume.astype(float)
        dfdict = Util.resample_to_dict(df_ta)
        return dfdict

    def write_data(df, table_name, engine):
        # Delete all content from tables
        with spot_engine.connect() as con:
            con.execute(f"DELETE FROM \"{table_name}\"")

        # Write dataframes to tables
        print("writing dataframe to spot table")
        df = df.sort_values(by="starttime").reset_index(drop="true")
        df.to_sql(table_name, engine, if_exists='append', index=True)

class Strategy():
    def __init__(self, df) -> None:
        self.df = df
        self.window = 14
        self.smooth_window = 3
        self.lags = 7
    
    def _calculate_indicators(self):
        self.df['%K'] = ta.momentum.stoch(self.df.high, self.df.low, self.df.close, window=self.window, smooth_window=self.smooth_window)
        self.df['%D'] = self.df['%K'].rolling(self.smooth_window).mean()
        self.df['rsi'] = ta.momentum.rsi(self.df.close, window=self.window)
        self.df['macd'] = ta.trend.macd_diff(self.df.close)
        self.df.dropna(inplace=True)

    def _calculate_stoch_triggers(self, lags, buy):
        dfx = pd.DataFrame()
        for i in range(1,lags+1):
            if buy:
                mask = (self.df['%K'].shift(i) < 20) & (self.df['%D'].shift(i) < 20)
            else:
                mask = (self.df['%K'].shift(i) > 80) & (self.df['%D'].shift(i) > 80)
            dfx = dfx.append(mask, ignore_index=True)
        return dfx.sum(axis=0)

    def _calculate_execution_dates(self):
        buy_dates, sell_dates = [], []

        for i in range(len(self.df) - 1):
            if self.df.buy.iloc[i]:
                buy_dates.append(self.df.iloc[i + 1].name)
                for num, j in enumerate(self.df.sell[i:]):
                    if j:
                        sell_dates.append(self.df.iloc[i + num + 1].name)
                        break
        
        # remove buy signals that did not get sold yet                     
        cutit = len(buy_dates) - len(sell_dates)
        if cutit:
            buy_dates = buy_dates[:-cutit]

        frame = pd.DataFrame({'buy_dates': buy_dates, 'sell_dates': sell_dates})
        # remove overlapping trades (only one position at a time)
        actuals = frame[frame.buy_dates > frame.sell_dates.shift(1)]

        # integrate dates we bought and sold at into the dataframe
        self.df['bought'] = 0
        self.df.loc[actuals['buy_dates'],'bought'] = 1
        self.df['sold'] = 0
        self.df.loc[actuals['sell_dates'],'sold'] = 1
        return actuals

    def _calculate_profit(self):
        self.df['profit'] = 0
        buy_price = 0
        sell_price = 0
        for i in range(len(self.df)):
            line = self.df.iloc[i]
            if line["bought"]:
                buy_price = line["open"]
            if line["sold"]:
                sell_price = line["open"]
                self.df.loc[line.name, 'profit'] = (sell_price - buy_price) / buy_price
                buy_price = 0
                sell_price = 0
    
    def execute_strategy(self):
        self._calculate_indicators()
        self.df['kd_buy_trigger'] = np.where(self._calculate_stoch_triggers(self.lags, True), 1, 0)
        self.df['kd_sell_trigger'] = np.where(self._calculate_stoch_triggers(self.lags, False), 1, 0)
        self.df['buy'] = np.where((self.df['kd_buy_trigger']) & (self.df['%K'].between(20,80)) & (self.df['%D'].between(20,80)) & (self.df.rsi > 50) & (self.df.macd > 0), 1 , 0)
        self.df['sell'] = np.where((self.df['kd_sell_trigger']) & (self.df['%K'].between(20,80)) & (self.df['%D'].between(20,80)) & (self.df.rsi < 50) & (self.df.macd < 0), 1 , 0)
        self._calculate_execution_dates()
        self._calculate_profit()
        return self.df


def run_strategy():
    dfdict = Util.load_data(COIN_VIEW, engine)
    first = True
    for key in dfdict:
        macd = Strategy(dfdict[key].copy())
        df = macd.execute_strategy()
        if first:
            df_all = df
            first = False
        else:
            df_all = df_all.append(df)

    df = df_all.loc[:, ["open",	"high",	"low", "close",	"quotevolume", "symbol", "%K", "%D", "rsi", "macd", "bought", "sold", "profit"]].reset_index()
    print(
        df.loc[df.profit != 0, :].groupby("symbol").agg({
            "profit": ["sum", "mean"]
        })
    )
    Util.write_data(df, COIN_TA_TABLE_NAME, spot_engine)