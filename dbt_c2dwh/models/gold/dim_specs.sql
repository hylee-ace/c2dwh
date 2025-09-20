{{
  config(
    external_location = 's3://crawling-to-dwh/gold/dim_specs/',
    )
}}

{% set tables = list_tables('c2dwh_silver') %}
with temp as(
{% for i in tables %}
    {% set columns = list_specs_columns(i[1],i[0]) %}
    {% for j in columns %}
        select sku product_sku,
            '{{j}}' specs_name,
            try_cast({{j}} as varchar) specs_value,
            typeof({{j}}) data_type
        from {{i[0]}}.{{i[1]}}
        {% if not loop.last %}
            union
        {% endif %}
    {% endfor %}
    {% if not loop.last %}
        union
    {% endif %}
{% endfor %}
)
select row_number()over(order by product_sku, specs_name) id, 
    product_sku, specs_name, specs_value, data_type
from temp