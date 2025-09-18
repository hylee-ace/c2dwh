{{
  config(
    partitioned_by=['partition_date'],
    external_location='s3://crawling-to-dwh/silver/phones/',
    )
}}

with temp as(
	select cast(sku as int) sku,
		trim(
			regexp_replace(
				name,
				'^Điện thoại (định vị trẻ em)?|\d+(GB|TB).*',
				''
			)
		) name,
		case
			when price = '0' then null else cast(price as int)
		end price,
		case
			when brand = 'iPhone (Apple)' then 'Apple' else brand
		end brand,
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
			when cpu = 'Đang cập nhật'
			or cpu = '' then null else trim(regexp_replace(cpu, '\d+\s*nhân', ''))
		end cpu,
		cast(
			regexp_extract(cpu_speed, '(\d+\.?\d*)\s*GHz', 1) as double
		) cpu_speed_ghz_max,
		case
			when gpu = 'Hãng không công bố'
			or gpu = '' then null else trim(
				regexp_replace(gpu, '\d+\.?\d*\s*GHz|\d+\s*nhân', '')
			)
		end gpu,
		case
			regexp_like(ram, 'MB')
			when true then round(
				cast(regexp_extract(ram, '\d+') as double) / 1024,
				2
			) else try_cast(regexp_extract(ram, '\d+') as double)
		end ram_gb,
		case
			when regexp_like(storage, 'TB') then cast(regexp_extract(storage, '\d+') as double) * 1024
			when regexp_like(storage, 'GB') then cast(regexp_extract(storage, '\d+') as double)
			when regexp_like(storage, 'MB') then round(
				cast(regexp_extract(storage, '\d+') as double),
				2
			) / 1024 else null
		end storage_gb,
		case
			regexp_like(rearcam_specs, 'MP')
			when true then array_join(
				regexp_extract_all(rearcam_specs, '\d+\.?\d*\s*MP'),
				' + '
			) else null
		end cam_res_rear,
		case
			regexp_like(frontcam_specs, 'MP')
			when true then array_join(
				regexp_extract_all(frontcam_specs, '\d+\.?\d*\s*MP'),
				' + '
			) else null
		end cam_res_front,
		case
			when screen_type = 'Hãng không công bố'
			or screen_type = '' then null else screen_type
		end screen_type,
		round(
			cast(
				element_at(
					regexp_extract_all(screen_size, '(\d+\.?\d*)\s*"', 1),
					1
				) as double
			),
			2
		) screen_size_inch_main,
		case
			regexp_like(screen_size, 'Phụ')
			when true then cast(
				element_at(
					regexp_extract_all(screen_size, '(\d+\.?\d*)\s*"', 1),
					2
				) as double
			) else null
		end screen_size_inch_sub,
		case
			regexp_like(screen_res, 'Phụ')
			when true then trim(
				regexp_replace(
					element_at(regexp_split(screen_res, '(x|&)\s*Phụ'), 1),
					'Chính',
					''
				)
			) else (
				case
					when screen_res = '' then null else screen_res
				end
			)
		end screen_res_main,
		case
			regexp_like(screen_res, 'Phụ')
			when true then trim(
				element_at(regexp_split(screen_res, '(x|&)\s*Phụ'), 2)
			) else null
		end screen_res_sub,
		case
			regexp_like(screen_panel, 'Phụ')
			when true then trim(
				regexp_replace(
					element_at(regexp_split(screen_panel, ',?\s*Phụ'), 1),
					'Chính',
					''
				)
			) else (
				case
					when screen_panel = ''
					or screen_panel = 'Hãng không công bố' then null else screen_panel
				end
			)
		end screen_panel_main,
		case
			regexp_like(screen_panel, 'Phụ')
			when true then trim(
				element_at(regexp_split(screen_panel, ',?\s*Phụ'), 2)
			) else null
		end screen_panel_sub,
		case
			when regexp_count(screen_rate, 'Hz') > 1 then cast(
				regexp_extract_all(screen_rate, '(\d+)\s*Hz', 1) [ 1 ] as int
			) else cast(
				regexp_extract(screen_rate, '(\d+)\s*Hz', 1) as int
			)
		end screen_rate_hz_main,
		case
			when regexp_count(screen_rate, 'Hz') > 1 then cast(
				regexp_extract_all(screen_rate, '(\d+)\s*Hz', 1) [ 2 ] as int
			) else null
		end screen_rate_hz_sub,
		case
			when regexp_count(screen_nits, 'nits') > 1 then cast(
				regexp_extract_all(screen_nits, '\d+') [ 1 ] as int
			) else cast(regexp_extract(screen_nits, '\d+') as int)
		end screen_nits_main,
		case
			when regexp_count(screen_nits, 'nits') > 1 then cast(
				regexp_extract_all(screen_nits, '\d+') [ 2 ] as int
			) else null
		end screen_nits_sub,
		case
			when os = '' then null else trim(regexp_replace(os, '\(.*\)', ''))
		end os,
		regexp_extract(water_resistant, 'IP(X)?\d+') water_resistant,
		cast(regexp_extract(battery, '(\d+)\s*mAh', 1) as int) battery_mah,
		cast(
			regexp_extract(charger, '(\d+\.?\d*)\s*W', 1) as double
		) charger_w,
		cast(regexp_extract(weight, '\d+\.?\d*') as double) weight_g,
		case
			when material = '' then null else material
		end material,
		case
			when connectivity = '' then null else regexp_replace(connectivity, 'v(\d+\.?\d*)', 'Bluetooth $1')
		end connectivity,
		regexp_extract(network, '(\d+G(\s*VoLTE)?)', 1) network_support,
		case
			when ports = '' then null else regexp_replace(ports, '(3.5\s*mm)', 'Jack $1')
		end ports,
		row_number() over(
			partition by sku, partition_date
			order by updated_at desc
		) latest,
		partition_date
	from {{ source('bronze', 'phones') }}
)
select sku,
	name,
	price,
	brand,
	category,
	rating,
	reviews_count,
	cpu,
	cpu_speed_ghz_max,
	gpu,
	ram_gb,
	storage_gb,
	cam_res_rear,
	cam_res_front,
	screen_type,
	screen_size_inch_main,
	screen_size_inch_sub,
	screen_res_main,
	screen_res_sub,
	screen_panel_main,
	screen_panel_sub,
	screen_rate_hz_main,
	screen_rate_hz_sub,
	screen_nits_main,
	screen_nits_sub,
	os,
	water_resistant,
	battery_mah,
	charger_w,
	weight_g,
	material,
	connectivity,
	network_support,
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