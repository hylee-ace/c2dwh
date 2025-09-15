create external table if not exists c2dwh_gold.dim_products (
	sku int,
	name string,
	brand string,
	category string,
	release_year int,
	release_month int,
	url string
)
stored as parquet
location 's3://crawling-to-dwh/gold/dim_products/'
tblproperties ('parquet.compression' = 'SNAPPY');