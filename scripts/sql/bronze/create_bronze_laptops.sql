CREATE EXTERNAL TABLE IF NOT EXISTS c2dwh_bronze.laptops (
	sku string,
	name string,
	price string,
	brand string,
	category string,
	rating string,
	reviews_count string,
	url string,
	release_date string,
	updated_at string,
	cpu string,
	cpu_cores string,
	cpu_threads string,
	cpu_speed string,
	gpu string,
	ram string,
	max_ram string,
	ram_type string,
	ram_bus string,
	storage string,
	webcam string,
	screen_panel string,
	screen_size string,
	screen_tech string,
	screen_res string,
	screen_rate string,
	screen_nits string,
	os string,
	battery string,
	weight string,
	material string,
	connectivity string,
	ports string
)
PARTITIONED BY (date date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
	'separatorChar' = ',',
	'quoteChar' = '"',
	'escapeChar' = '\\',
	'serialization.null.format' = '""'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://crawling-to-dwh/bronze/laptops/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);