create external table if not exists c2dwh_gold.dim_specs(
	id int,
	product_sku int,
	specs_name string,
	numeric_value double,
	string_value string
)
stored as parquet
location 's3://crawling-to-dwh/gold/dim_specs/'
tblproperties ('parquet.compression' = 'SNAPPY');