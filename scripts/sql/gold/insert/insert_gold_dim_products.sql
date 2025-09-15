insert into c2dwh_gold.dim_products
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.phones
where date = current_date
union
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.laptops
where date = current_date
union
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.tablets
where date = current_date
union
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.watches
where date = current_date
union
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.earphones
where date = current_date
union
select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
from c2dwh_silver.screens
where date = current_date