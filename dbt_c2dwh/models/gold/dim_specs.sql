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
            try_cast(case
                when typeof({{j}}) = 'varchar' then {{j}} else null
            end as varchar) string_value,
            try_cast(case
                when typeof({{j}}) != 'varchar' then {{j}} else null
            end as double) numeric_value
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
    product_sku, specs_name, string_value, numeric_value
from temp