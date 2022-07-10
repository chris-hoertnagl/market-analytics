#Precious Metals:
PMETAL_REQUEST_URL='https://www.gold.de/api/metalle-intraday.php?func=intraday&_='
PMETAL_MARKETCAP_URL='https://8marketcap.com/metals/euro'
PMETAL_PATTERN=r'"au_gold_eur":"(.*?) EUR",.*"au_silber_eur":"(.*?) EUR",.*"au_platin_eur":"(.*?) EUR",.*"au_palladium_eur":"(.*?) EUR",'
PMETAL_MARKETCAP_PATTERN=r'<td class="td-right"><span class="badge badge-metal">(\w*?)<\/span><\/td>\s*<td class="td-right" data-sort=".*?">\$(.*?) (\w)<\/td>.*?<td class="td-right"><span class="badge badge-metal">(\w*?)<\/span><\/td>\s*<td class="td-right" data-sort=".*?">\$(.*?) (\w)<\/td>.*?<td class="td-right"><span class="badge badge-metal">(\w*?)<\/span><\/td>\s*<td class="td-right" data-sort=".*?">\$(.*?) (\w)<\/td>.*?<td class="td-right"><span class="badge badge-metal">(\w*?)<\/span><\/td>\s*<td class="td-right" data-sort=".*?">\$(.*?) (\w)<\/td>.*?'

# Staging tables
PMETAL_TABLE_NAME='metaltable'
CRYPTO_TABLE_NAME = "cryptotable"
SHARE_TABLE_NAME = "sharetable"

# List of assets considered
COINS = ["BTC", "ETH", "ADA", "BNB", "XRP", "SOL", "DOGE", "UNI", "MATIC", "TRX"]
STOCKS = ["TSLA", "AAPL", "MSFT", "GOOG", "TSM", "BABA", "NFLX", "AMZN", "PYPL"]
PMETALS = ["GOLD","SILVER","PLAT","PALLAD"]

# Analize all assets
ALL_ASSETS_VIEW = "allassetsview"
DAILY_INDEX_TABLE_NAME = "dailyindextable"
DAILY_TABLE_NAME = "dailytable"
DAILY_INDEX_VIEW = "allassetsindexview"
DAILY_VIEW = "allassetsdailyview"

# Coin trading
COIN_VIEW = "coinview"
COIN_TA_TABLE_NAME = "cointradetable"

# Share trading
SHARE_VIEW = "sharedailyview"
SHARE_TA_TABLE_NAME = "sharetradetable"