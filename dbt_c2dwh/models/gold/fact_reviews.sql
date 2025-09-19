{{
  config(
    materialized = 'incremental',
    external_location = 's3://crawling-to-dwh/gold/fact_reviews/',
    partitioned_by = ['partition_date'],
    )
}}

{% set tables = list_tables('c2dwh_silver') %}
with temp as(
{% for i in tables %}
    select sku product_sku,
        rating,
        reviews_count,
        price onsale_price,
        cast(regexp_replace(partition_date, '-', '') as int) date_id,
        updated_at,
        partition_date
    from {{i[0]}}.{{i[1]}}
    where partition_date = cast(current_date as varchar)
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
)
select row_number()over(order by product_sku, updated_at) id, 
    product_sku, rating, reviews_count, onsale_price, date_id, updated_at, partition_date
from temp