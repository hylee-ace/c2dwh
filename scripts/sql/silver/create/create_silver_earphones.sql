create external table if not exists c2dwh_silver.earphones (
	sku int,
	name string,
	price int,
	brand string,
	category string,
	rating double,
	reviews_count int,
	sound_tech string,
	compatible string,
	control string,
	connectivity string,
	water_resistant string,
	ports string,
	runtime_hrs double,
	recharge_hrs double,
	case_runtime_hrs double,
	case_recharge_hrs double,
	weight_g double,
	url string,
	release_year int,
	release_month int,
	updated_at timestamp
)
partitioned by (partition_date string)
stored as parquet
location 's3://crawling-to-dwh/silver/earphones/'
tblproperties ('parquet.compression' = 'SNAPPY');