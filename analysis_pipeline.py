from db_connector import engine, spot_engine
import pandas as pd
from config import DAILY_INDEX_TABLE_NAME, DAILY_TABLE_NAME, ALL_ASSETS_VIEW

def fill_resample(df_in):
    dfdict = dict(tuple(df_in.groupby("symbol")))
    df_in = None
    first = True
    for key in dfdict:
        df = dfdict[key]
        df = df.sort_values("time")
        df = df.fillna(method="ffill")
        df = df.fillna(method="bfill")

        df = df.resample('1D', on="time").agg({
            "price": "last",
            "market_cap": "last",
            "symbol": "last",
            "asset_type": "last"
        })

        df["price_change"] = df.price.pct_change()
        df["market_cap_change"] = df.market_cap.pct_change()
        df["price_change_cumulated"] = df["price_change"].cumsum()

        if first:
            dfv_all = df
            first = False
        else:
            dfv_all = dfv_all.append(df)
        
    return dfv_all.reset_index()

def get_index_change(df_in):
    dfdict = dict(tuple(df_in.groupby("asset_type")))
    first = True
    for key in dfdict:
        df = dfdict[key]
        df = df.sort_values("time")
        df["index_change_cumulated"] = df.index_change.cumsum()

        if first:
            dfv_all = df
            first = False
        else:
            dfv_all = dfv_all.append(df)
        
    return dfv_all

def pipeline():
    print("reading all assets from view")
    # Load data an resample
    df_daily = pd.read_sql(f"select * from \"{ALL_ASSETS_VIEW}\"", engine.connect())
    df_daily = fill_resample(df_daily)
    df_daily = df_daily.fillna(0)

    # Calculate index weights
    df_market_sum = df_daily.groupby(["time", "asset_type"])["market_cap"].sum().reset_index()
    df_daily = df_daily.merge(df_market_sum, left_on=["time", "asset_type"], right_on=["time", "asset_type"], how="left")
    df_daily.rename(columns={'market_cap_x':'market_cap', 'market_cap_y':'market_cap_type_sum'}, inplace=True)
    df_daily["index_weight"] = df_daily.market_cap / df_daily.market_cap_type_sum
    df_daily["index_change"] = df_daily.price_change * df_daily.index_weight
    df_daily["index_value"] = df_daily.price * df_daily.index_weight
    

    df_daily_indexed = df_daily.groupby(["time", "asset_type"]).agg({
        "market_cap": "sum",
        "market_cap_change": "mean",
        "index_change": "sum",
        "index_weight": "sum",
        "index_value": "sum"
    })

    df_daily_indexed = get_index_change(df_daily_indexed.reset_index())
    df_daily_indexed = df_daily_indexed.fillna(0)

    df_daily = df_daily.loc[:,["time", "asset_type", "symbol", "price", "price_change", "price_change_cumulated", "market_cap", "market_cap_change", "index_weight"]]
    
    df_daily = df_daily.sort_values(by="time").reset_index(drop="true")
    df_daily_indexed = df_daily_indexed.sort_values(by="time").reset_index(drop="true")

    # Delete all content from tables
    with spot_engine.connect() as con:
        con.execute(f"DELETE FROM \"{DAILY_TABLE_NAME}\"")
        con.execute(f"DELETE FROM \"{DAILY_INDEX_TABLE_NAME}\"")

    # Write dataframes to tables
    print("writing dataframes to spot tables")
    df_daily.to_sql(DAILY_TABLE_NAME, spot_engine, if_exists='append', index=True)
    df_daily_indexed.to_sql(DAILY_INDEX_TABLE_NAME, spot_engine, if_exists='append', index=True)
    print("anlysis pipeline finished")
    
    