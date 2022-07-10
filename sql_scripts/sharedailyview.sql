create view postgres.cryptolytics_staging.sharedailyview AS
SELECT cast(ct."starttime" as date) AS "date", open_price_per_day_and_asset.first_open as "open", ct."close", highest_price_per_day_and_asset.max_high as "high", lowest_price_per_day_and_asset.min_low as "low", latest_time_per_day_and_asset.basevolume_day as "basevolume", latest_time_per_day_and_asset.tradecount_day as "tradecount", ct.market_cap, ct.symbol
FROM cryptolytics_staging.sharetable ct
INNER JOIN
(
    SELECT symbol as a_symbol, MAX("starttime") AS max_trade_time, SUM("basevolume") as basevolume_day, SUM("tradecount") as tradecount_day
    FROM cryptolytics_staging.sharetable
    GROUP BY DATE(starttime), symbol
) latest_time_per_day_and_asset
    ON
       latest_time_per_day_and_asset.a_symbol = ct.symbol and
       latest_time_per_day_and_asset.max_trade_time = ct."starttime" 
INNER JOIN
(
    SELECT symbol as h_symbol, DATE(starttime) as h_date, MAX("high") AS max_high
    FROM cryptolytics_staging.sharetable
    GROUP BY DATE(starttime), symbol
) highest_price_per_day_and_asset
    ON
       highest_price_per_day_and_asset.h_symbol = ct.symbol and
       highest_price_per_day_and_asset.h_date = DATE(ct."starttime") 
INNER JOIN
(
    SELECT symbol as l_symbol, DATE(starttime) as l_date, MIN("low") AS min_low
    FROM cryptolytics_staging.sharetable
    GROUP BY DATE(starttime), symbol
) lowest_price_per_day_and_asset
    ON
       lowest_price_per_day_and_asset.l_symbol = ct.symbol and
       lowest_price_per_day_and_asset.l_date = DATE(ct."starttime") 
INNER JOIN
(
    SELECT symbol as f_symbol, cast(open_ht."starttime" as date) AS f_date, open_ht."open" as first_open
    FROM cryptolytics_staging.sharetable open_ht
    INNER JOIN
    (
        SELECT symbol as o_symbol, MIN("starttime") AS min_trade_time
        FROM cryptolytics_staging.sharetable
        GROUP BY DATE(starttime), symbol
    ) first_time_per_day_and_asset
        ON
        first_time_per_day_and_asset.o_symbol = open_ht.symbol and
        first_time_per_day_and_asset.min_trade_time = open_ht."starttime" 
) open_price_per_day_and_asset
    ON
       open_price_per_day_and_asset.f_symbol = ct.symbol and
       open_price_per_day_and_asset.f_date = DATE(ct."starttime") 