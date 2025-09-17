insert into c2dwh_silver.screens with temp as(
		select cast(sku as int) sku,
			trim(
				regexp_replace(
					name,
					'Màn hình (Gaming)?|\d+\.?\d*\s*inch.*|\(.*\)',
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
				when screen_type = '' then null else screen_type
			end screen_type,
			cast(
				regexp_extract(screen_size, '\d+\.?\d*') as double
			) screen_size_inch,
			case
				when screen_panel = ''
				or screen_panel = 'Hãng không công bố' then null else screen_panel
			end screen_panel,
			case
				when screen_tech = '' then null else screen_tech
			end screen_tech,
			case
				when screen_res = '' then null else screen_res
			end screen_res,
			cast(regexp_extract(screen_rate, '\d+') as int) screen_rate_hz,
			cast(
				regexp_extract(power_consumption, '\d+\.?\d*') as double
			) power_consumption_watt,
			case
				when ports = '' then null else ports
			end ports,
			cast(regexp_extract(weight, '\d+\.?\d*') as double) weight_kg,
			row_number() over(
				partition by sku, partition_date
				order by updated_at desc
			) latest,
			partition_date
		from c2dwh_bronze.screens
	)
select sku,
	name,
	price,
	brand,
	category,
	rating,
	reviews_count,
	screen_type,
	screen_panel,
	screen_size_inch,
	screen_tech,
	screen_res,
	screen_rate_hz,
	power_consumption_watt,
	ports,
	weight_kg,
	url,
	release_year,
	release_month,
	updated_at,
	partition_date
from temp
where latest = 1
	and partition_date = cast(current_date as varchar)
order by sku