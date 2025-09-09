CREATE EXTERNAL TABLE IF NOT EXISTS c2dwh_bronze.tablets (
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
	cpu_speed string,
	gpu string,
	ram string,
	storage string,
	rearcam_specs string,
	frontcam_specs string,
	screen_size string,
	screen_panel string,
	screen_res string,
	screen_rate string,
	os string,
	water_resistant string,
	battery string,
	charger string,
	weight string,
	material string,
	connectivity string,
	network string,
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
LOCATION 's3://crawling-to-dwh/bronze/tablets/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);