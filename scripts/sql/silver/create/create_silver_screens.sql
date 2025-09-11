create external table if not exists c2dwh_silver.screens(
	sku int,
	name string,
	price int,
	brand string,
	category string,
	rating double,
	reviews_count int,
	screen_type string,
	screen_panel string,
	screen_size_inch double,
	screen_tech string,
	screen_res string,
	screen_rate_hz int,
	power_consumption_watt double,
	ports string,
	weight_kg double,
	url string,
	release_year int,
	release_month int,
	updated_at timestamp
)
partitioned by (date date)
stored as parquet
location 's3://crawling-to-dwh/silver/screens/'
tblproperties('parquet.compression' = 'SNAPPY')