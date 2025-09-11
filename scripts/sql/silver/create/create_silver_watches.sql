create external table if not exists c2dwh_silver.watches(
	sku int,
	name string,
	price int,
	brand string,
	category string,
	rating double,
	reviews_count int,
	cpu string,
	storage_mb double,
	screen_type string,
	screen_size_inch double,
	screen_panel string,
	os string,
	water_resistant string,
	connectivity string,
	battery_mah double,
	weight_g double,
	material string,
	url string,
	release_year int,
	release_month int,
	updated_at timestamp
)
partitioned by (date date)
stored as parquet
location 's3://crawling-to-dwh/silver/watches/'
tblproperties('parquet.compression' = 'SNAPPY')