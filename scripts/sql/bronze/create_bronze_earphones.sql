create external table if not exists c2dwh_bronze.earphones (
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
partitioned by (date date)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
	'separatorChar' = ',',
	'quoteChar' = '"',
	'escapeChar' = '\\',
	'serialization.null.format' = '' -- this match to null value in csv file
)
stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location 's3://crawling-to-dwh/bronze/earphones/'
tblproperties (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);