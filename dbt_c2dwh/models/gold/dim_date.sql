{{
  config(
    external_location = 's3://crawling-to-dwh/gold/dim_date/',
    )
}}

{% set tables = list_tables('c2dwh_silver') %}
{% for i in tables %}
    select cast(regexp_replace(partition_date, '-', '') as int) id,
        cast(partition_date as date) full_date,
        year(cast(partition_date as date)) year,
        month(cast(partition_date as date)) month,
        day(cast(partition_date as date)) day
    from {{i[0]}}.{{i[1]}}
    {% if not loop.last %}
        union
    {% endif %}
{% endfor %}
