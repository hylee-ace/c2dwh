create external table if not exists c2dwh_silver.tablets (
	sku int,
	name string,
	price int,
	brand string,
	category string,
	rating double,
	reviews_count int,
	cpu string,
	cpu_speed_ghz_max double,
	gpu string,
	ram_gb int,
	storage_gb int,
	cam_res_rear string,
	cam_res_front string,
	screen_size_inch double,
	screen_panel string,
	screen_res_px string,
	screen_rate_hz int,
	os string,
	water_resistant string,
	battery_mah int,
	charger_w double,
	weight_g double,
	material string,
	connectivity string,
	network_support string,
	ports string,
	url string,
	release_year int,
	release_month int,
	updated_at timestamp
)
partitioned by (partition_date string)
stored as parquet
location 's3://crawling-to-dwh/silver/tablets/'
tblproperties ('parquet.compression' = 'SNAPPY');