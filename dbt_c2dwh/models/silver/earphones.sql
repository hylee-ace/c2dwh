{{
  config(
    partitioned_by = ['partition_date'],
    external_location = 's3://crawling-to-dwh/silver/earphones/',
    )
}}

with temp as(
    select cast(sku as int) sku,
        trim(
            regexp_replace(
                name,
                '(?i)tai nghe|bluetooth|chụp tai|gaming|open\s?-\s?ear|true wireless|thể thao|có dây|truyền âm thanh qua không khí|truyền xương|dẫn khí truyền âm',
                ''
            )
        ) name,
        case
            when price = '0' then null else cast(price as int)
        end price,
        brand,
        category,
        case
            when rating = '' then null else cast(rating as double)
        end rating,
        case
            when reviews_count = '' then null else cast(reviews_count as int)
        end reviews_count,
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
            when sound_tech = ''
            or sound_tech = 'Hãng không công bố' then null else sound_tech
        end sound_tech,
        case
            when compatible = '' then null else compatible
        end compatible,
        case
            when control = '' then null else control
        end control,
        case
            when connectivity = '' then null else connectivity
        end connectivity,
        case
            when water_resistant = '' then null else water_resistant
        end water_resistant,
        case
            when ports = '' then null else regexp_replace(ports, '(3.5 mm)', 'Jack $1')
        end ports,
        cast(
            regexp_extract(battery, 'Dùng.*?(\d+\.?\d*).*giờ.*', 1) as double
        ) runtime_hrs,
        case
            when regexp_extract(battery, 'Sạc.*?(\d+\.?\d*).*giờ.*', 1) is not null then cast(
                regexp_extract(battery, 'Sạc.*?(\d+\.?\d*).*giờ.*', 1) as double
            )
            when regexp_extract(battery, 'Sạc.*?(\d+\.?\d*).*phút.*', 1) is not null then round(
                cast(
                    regexp_extract(battery, 'Sạc.*?(\d+\.?\d*).*phút.*', 1) as double
                ) / 60,
                2
            ) else null
        end recharge_hrs,
        cast(
            regexp_extract(case_battery, 'Dùng.*?(\d+\.?\d*).*giờ.*', 1) as double
        ) case_runtime_hrs,
        case
            when regexp_extract(case_battery, 'Sạc.*?(\d+\.?\d*).*giờ.*', 1) is not null then cast(
                regexp_extract(case_battery, 'Sạc.*?(\d+\.?\d*).*giờ.*', 1) as double
            )
            when regexp_extract(case_battery, 'Sạc.*?(\d+\.?\d*).*phút.*', 1) is not null then round(
                cast(
                    regexp_extract(case_battery, 'Sạc.*?(\d+\.?\d*).*phút.*', 1) as double
                ) / 60,
                2
            ) else null
        end case_recharge_hrs,
        case
            when weight = '' then null else cast(
                regexp_extract(weight, '(\d+\.?\d*)±?.*', 1) as double
            )
        end weight_g,
        row_number() over(
            partition by sku, partition_date
            order by updated_at desc
        ) latest,
        partition_date
    from {{ source('bronze', 'earphones') }} -- still can use c2dwh.earphones 
)
select sku,
	name,
	price,
	brand,
	category,
	rating,
	reviews_count,
	sound_tech,
	compatible,
	control,
	connectivity,
	water_resistant,
	ports,
	runtime_hrs,
	recharge_hrs,
	case_runtime_hrs,
	case_recharge_hrs,
	weight_g,
	url,
	release_year,
	release_month,
	updated_at,
	partition_date
from temp
where latest = 1 and partition_date = cast(current_date as varchar) -- this filter to ensure athena not recreate all partition in s3
order by sku