{{
  config(
    external_location = 's3://crawling-to-dwh/gold/dim_product/',
    )
}}

{% set tables = list_tables('c2dwh_silver') %}
{% for i in tables %}
	select sku,
	name,
	brand,
	category,
	release_year,
	release_month,
	url
	from {{i[0]}}.{{i[1]}}
	where partition_date = cast(current_date as varchar)
	{% if not loop.last %}
	union
	{% endif %}
{% endfor %}
