insert into c2dwh_gold.dim_dates
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.phones
where date = current_date
union
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.laptops
where date = current_date
union
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.tablets
where date = current_date
union
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.watches
where date = current_date
union
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.earphones
where date = current_date
union
select cast(
		regexp_replace(cast(date as varchar), '-', '') as int
	) id,
	date full_date,
	year(date) year,
	month(date) month,
	day(date) day
from c2dwh_silver.screens
where date = current_date