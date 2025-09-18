{{
  config(
    partitioned_by=['partition_date'],
    external_location='s3://crawling-to-dwh/silver/laptops/',
    )
}}

with temp as(
	select cast(sku as int) sku,
		case
			regexp_like(name, '-')
			when true then trim(
				regexp_replace(
					element_at(regexp_split(name, '-'), 1),
					'^Laptop\s+',
					''
				)
			) else trim(
				regexp_replace(
					name,
					'((\d+GB/\d+(TB|GB)).*)|Apple|^Laptop\s',
					''
				)
			)
		end name,
		case
			when price = '0' then null else cast(price as int)
		end price,
		brand,
		category,
		try_cast(rating as double) rating,
		try_cast(reviews_count as int) reviews_count,
		case
			regexp_like(cpu, 'Hãng không công bố')
			when true then regexp_replace(cpu, '\s*-\s*Hãng không công bố.*', '') else cpu
		end cpu,
		try_cast(cpu_cores as int) cpu_cores,
		try_cast(cpu_threads as int) cpu_threads,
		case
			regexp_like(cpu_speed, '^(\d\.\d*\s*GHz).*')
			when true then cast(
				regexp_extract(cpu_speed, '^(\d\.\d*)\s*GHz.*', 1) as double
			) else null
		end cpu_speed_ghz,
		case
			regexp_like(cpu_speed, '.*Lên đến\s(\d\.\d*\s*GHz).*')
			when true then cast(
				regexp_extract(
					cpu_speed,
					'.*Lên đến\s(\d\.\d*)\s*GHz.*',
					1
				) as double
			) else null
		end cpu_speed_ghz_max,
		trim(
			regexp_replace(
				element_at(regexp_split(gpu, '-'), 2),
				',\s*\d+\s*GB|\(.*\)',
				''
			)
		) gpu,
		case
			trim(element_at(regexp_split(gpu, '-'), 1))
			when 'Card rời' then 'Discrete' else 'Integrated'
		end gpu_type,
		case
			regexp_like(gpu, '\d+\s*GB')
			when true then cast(regexp_extract(gpu, '(\d+)\s*GB', 1) as int) else null
		end gpu_ram_gb,
		try_cast(regexp_extract(ram, '\d+') as int) ram_gb,
		try_cast(regexp_extract(max_ram, '\d+') as int) ram_gb_max,
		case
			when ram_type = 'Hãng không công bố'
			or ram_type = '' then null else trim(regexp_replace(ram_type, '\(.*\)', ''))
		end ram_type,
		case
			regexp_like(ram_bus, '\d+\s*MHz')
			when true then cast(
				regexp_extract(ram_bus, '(\d+)\s*\MHz', 1) as int
			) else null
		end ram_bus_mhz,
		case
			regexp_extract(storage, '^\d+\s*(TB|GB)', 1)
			when 'TB' then cast(regexp_extract(storage, '^\d+') as int) * 1024 else cast(regexp_extract(storage, '^\d+') as int)
		end disk_gb,
		case
			regexp_like(storage, 'tối đa \d+\s*(TB|GB)')
			when true then cast(
				regexp_extract(storage, 'tối đa (\d+)\s*TB', 1) as int
			) * 1024 else null
		end disk_gb_max,
		case
			regexp_like(storage, ',|\(.*\)')
			when true then trim(
				regexp_extract(storage, '^\d+\s*(TB|GB)(.*?)(,|\s*\(.*)', 2)
			) else trim(regexp_extract(storage, '^\d+\s*(TB|GB)(.*)', 2))
		end disk_type,
		case
			when webcam = '' then null else webcam
		end webcam,
		case
			when screen_panel = '' then null else screen_panel
		end screen_panel,
		try_cast(
			regexp_extract(screen_size, '\d+\.?\d*') as double
		) screen_size_inch,
		case
			when screen_tech = '' then null else screen_tech
		end screen_tech,
		case
			when screen_res = '' then null else screen_res
		end screen_res,
		try_cast(
			regexp_extract(screen_rate, '(\d+)\s*Hz', 1) as int
		) screen_rate_hz,
		try_cast(regexp_extract(screen_nits, '\d+') as int) screen_nits,
		regexp_replace(os, '\s*\+.*', '') os,
		try_cast(
			regexp_extract(battery, '(\d+\.?\d*)\s*Wh', 1) as double
		) battery_wh,
		round(
			try_cast(
				regexp_extract(weight, '(\d+\.?\d*)\s*kg', 1) as double
			),
			2
		) weight_kg,
		case
			when material = '' then null else material
		end material,
		case
			when connectivity = 'Đang cập nhật'
			or connectivity = '' then null else connectivity
		end connectivity,
		case
			when ports = '' then null else ports
		end ports,
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
		row_number() over(
			partition by sku, partition_date
			order by updated_at desc
		) latest,
		partition_date
	from {{ source('bronze', 'laptops') }}
)
select sku,
	name,
	price,
	brand,
	category,
	rating,
	reviews_count,
	cpu,
	cpu_cores,
	cpu_threads,
	cpu_speed_ghz,
	cpu_speed_ghz_max,
	gpu,
	gpu_ram_gb,
	gpu_type,
	ram_gb,
	ram_gb_max,
	ram_type,
	ram_bus_mhz,
	disk_gb,
	disk_gb_max,
	regexp_replace(disk_type, 'Gen\s*(\d\.?0?)', '$1') disk_type,
	webcam,
	screen_panel,
	screen_size_inch,
	screen_tech,
	screen_res,
	screen_rate_hz,
	screen_nits,
	os,
	battery_wh,
	weight_kg,
	material,
	connectivity,
	ports,
	url,
	release_year,
	release_month,
	updated_at,
	partition_date
from temp
where latest = 1 and partition_date in('2025-09-09','2025-09-17')
	{# and partition_date = cast(current_date as varchar) -- this filter to ensure athena not recreate all partition in s3 #}
order by sku