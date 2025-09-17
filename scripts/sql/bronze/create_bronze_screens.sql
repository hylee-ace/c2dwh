create external table if not exists c2dwh_bronze.screens (
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
	screen_type string,
	screen_panel string,
	screen_size string,
	screen_tech string,
	screen_res string,
	screen_rate string,
	power_consumption string,
	ports string,
	weight string
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
location 's3://crawling-to-dwh/bronze/screens/'
tblproperties (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);