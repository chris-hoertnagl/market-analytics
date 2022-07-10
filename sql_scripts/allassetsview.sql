CREATE OR REPLACE VIEW cryptolytics_staging.allassetsview as
select
	all_minutes_assets.starttime as time,
	all_assets.price,
	all_assets.market_cap,
	all_minutes_assets.symbol,
	all_minutes_assets.asset_type
from
	(
		SELECT
			ct.starttime as time,
			ct."close" as price,
			ct.market_cap,
			ct.symbol,
			'coin' as asset_type
		FROM
			cryptolytics_staging."cryptotable" ct
		union
		select
			st.starttime as time,
			st."close" as price,
			st.market_cap,
			st.symbol,
			'share' as asset_type
		from
			cryptolytics_staging."sharetable" st
		union
		select
			p.starttime as time,
			p."close" as price,
			p.market_cap,
			p.symbol,
			'metal' as asset_type
		from
			cryptolytics_staging."metaltable" p
	) as all_assets
	right join (
		(
			SELECT
				distinct(c.starttime),
				0 as del
			FROM
				cryptolytics_staging.cryptotable c
		) as all_minutes full
		outer join (
			select
				distinct(ct.symbol),
				0 as del,
				'coin' as asset_type
			FROM
				cryptolytics_staging."cryptotable" ct
			union
			select
				distinct(st.symbol),
				0 as del,
				'share' as asset_type
			from
				cryptolytics_staging."sharetable" st
			union
			select
				distinct(p.symbol),
				0 as del,
				'metal' as asset_type
			from
				cryptolytics_staging."metaltable" p
		) as all_symbols on all_minutes.del = all_symbols.del
	) as all_minutes_assets on all_minutes_assets.starttime = all_assets.time
	and all_minutes_assets.symbol = all_assets.symbol
