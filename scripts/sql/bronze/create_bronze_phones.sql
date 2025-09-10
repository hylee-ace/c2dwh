create external table if not exists c2dwh_bronze.phones (
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
	screen_type string,
	screen_size string,
	screen_panel string,
	screen_res string,
	screen_rate string,
	screen_nits string,
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
partitioned by (date date)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
	'separatorChar' = ',',
	'quoteChar' = '"',
	'escapeChar' = '\\',
	'serialization.null.format' = '' -- this match to null value in csv file
)
stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location 's3://crawling-to-dwh/bronze/phones/'
tblproperties (
	'classification' = 'csv',
	'skip.header.line.count' = '1'
);