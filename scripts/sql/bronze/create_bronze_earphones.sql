CREATE EXTERNAL TABLE IF NOT EXISTS c2dwh_bronze.earphones (
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
	sound_tech string,
	compatible string,
	control string,
	connectivity string,
	water_resistant string,
	ports string,
	battery string,
	case_battery string,
	weight string
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
LOCATION 's3://crawling-to-dwh/bronze/earphones/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);