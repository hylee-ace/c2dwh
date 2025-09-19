{{
  config(
    partitioned_by = ['partition_date'],
    external_location = 's3://crawling-to-dwh/silver/watches/',
    )
}}

with temp as(
	select cast(sku as int) sku,
		trim(
			regexp_replace(
				name,
				'Vòng đeo tay thông minh|Đồng hồ định vị trẻ em|Đồng hồ thông minh|Nhẫn thông minh|viền.*|dây.*|Đồng Hồ Định Vị Trẻ Em|Xanh.*|Hồng.*|\d+\.?\d*\s*mm',
				''
			)
		) name,
		case
			when price = '0' then null else cast(price as int)
		end price,
		brand,
		category,
		try_cast(rating as double) rating,
		try_cast(reviews_count as int) reviews_count,
		url,
		case
			when regexp_count(release_date, '/') > 0 then try_cast(
				regexp_split(release_date, '/') [ 2 ] as int
			) else try_cast(release_date as int)
		end release_year,
		case
			when regexp_count(release_date, '/') > 0 then try_cast(
				regexp_split(release_date, '/') [ 1 ] as int
			) else null
		end release_month,
		cast(updated_at as timestamp) updated_at,
		case
			when cpu = ''
			or cpu = 'Hãng không công bố'
			or regexp_like(cpu, 'GHz') then null else cpu
		end cpu,
		case
			when regexp_like(storage, 'GB') then cast(regexp_extract(storage, '\d+') as double) * 1024
			when regexp_like(storage, 'MB') then cast(regexp_extract(storage, '\d+') as double)
			when regexp_like(storage, 'KB') then round(
				cast(regexp_extract(storage, '\d+') as double) / 1024,
				2
			) else null
		end storage_mb,
		case
			when screen_type = '' then null else screen_type
		end screen_type,
		cast(
			regexp_extract(screen_size, '\d+\.?\d*') as double
		) screen_size_inch,
		case
			when screen_panel = '' then null else screen_panel
		end screen_panel,
		case
			when os = ''
			or os = 'Hãng không công bố' then null else regexp_replace(os, 'được.*|phiên.*', '')
		end os,
		regexp_extract(water_resistant, '\d+\s*ATM|IP(X)?\d+') water_resistant,
		case
			when connectivity = '' then null else regexp_replace(connectivity, 'v(\d+\.?\d*)', '$1')
		end connectivity,
		cast(
			regexp_extract(battery, '(\d+\.?\d*)\s*mAh', 1) as double
		) battery_mah,
		cast(regexp_extract(weight, '\d+\.?\d*') as double) weight_g,
		case
			when material = '' then null else material
		end material,
		row_number() over(
			partition by sku, partition_date
			order by updated_at desc
		) latest,
		partition_date
	from {{ source('bronze', 'watches') }}
)
select sku,
	name,
	price,
	brand,
	category,
	rating,
	reviews_count,
	cpu,
	storage_mb,
	screen_type,
	screen_size_inch,
	screen_panel,
	os,
	water_resistant,
	connectivity,
	battery_mah,
	weight_g,
	material,
	url,
	release_year,
	release_month,
	updated_at,
	partition_date
from temp
where latest = 1 and partition_date = cast(current_date as varchar)
order by sku