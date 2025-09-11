insert into c2dwh_silver.tablets with temp as(
		select cast(sku as int) sku,
			trim(
				regexp_replace(
					name,
					'^Máy tính bảng|\d+(GB|TB).*',
					''
				)
			) name,
			case
				when price = '0' then null else cast(price as int)
			end price,
			case
				when brand = 'iPad (Apple)' then 'Apple' else brand
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
			cast(regexp_extract(ram, '(\d+)\s*GB', 1) as int) ram_gb,
			case
				when regexp_like(storage, 'TB') then cast(regexp_extract(storage, '\d+') as int) * 1024
				when regexp_like(storage, 'GB') then cast(regexp_extract(storage, '\d+') as int) else null
			end storage_gb,
			case
				regexp_like(rearcam_specs, 'MP')
				when true then array_join(
					regexp_extract_all(rearcam_specs, '\d+\.?\d*\s*MP'),
					' + '
				) else null
			end rear_cam_mp,
			case
				regexp_like(frontcam_specs, 'MP')
				when true then array_join(
					regexp_extract_all(frontcam_specs, '\d+\.?\d*\s*MP'),
					' + '
				) else null
			end front_cam_mp,
			cast(
				regexp_extract(screen_size, '\d+\.?\d*') as double
			) screen_size_inch,
			case
				when screen_panel = 'Hãng không công bố'
				or screen_panel = '' then null else screen_panel
			end screen_panel,
			regexp_extract(screen_res, '\d+\s*x\s*\d+') screen_res_px,
			cast(
				regexp_extract(screen_rate, '(\d+)\s*Hz', 1) as int
			) screen_rate_hz,
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
				partition by sku
				order by updated_at desc
			) latest,
			date
		from c2dwh_bronze.tablets
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
	rear_cam_mp,
	front_cam_mp,
	screen_size_inch,
	screen_panel,
	screen_res_px,
	screen_rate_hz,
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
	date
from temp
where latest = 1
	and date = current_date -- this filter to ensure athena not recreate all partition in s3
order by sku