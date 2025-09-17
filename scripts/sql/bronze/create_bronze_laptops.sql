create external table if not exists c2dwh_bronze.laptops (
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
partitioned by (partition_date string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
	'separatorChar' = ',',
	'quoteChar' = '"',
	'escapeChar' = '\\',
	'serialization.null.format' = '' -- this match to null value in csv file
)
stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location 's3://crawling-to-dwh/bronze/laptops/'
tblproperties (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);