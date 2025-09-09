CREATE EXTERNAL TABLE IF NOT EXISTS c2dwh_bronze.watches (
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
	storage string,
	screen_type string,
	screen_size string,
	screen_panel string,
	os string,
	water_resistant string,
	connectivity string,
	battery string,
	weight string,
	material string
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
LOCATION 's3://crawling-to-dwh/bronze/watches/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);