create external table if not exists c2dwh_gold.dim_dates(
	id int,
	full_date date,
	year int,
	month int,
	day int
)
stored as parquet
location 's3://crawling-to-dwh/gold/dim_dates/'
tblproperties ('parquet.compression' = 'SNAPPY');