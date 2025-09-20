{# override builtin macro 'generate_schema_name' #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{# list tables from database #}
{% macro list_tables(schema) %}
    {% if execute %}
        {% set tables = [] %}
        {% for i in graph.nodes.values() %}
            {% if i.resource_type=='model' and i.schema==schema %}
                {% do tables.append((i.schema,i.alias)) %}
            {% endif %}
        {% endfor %}
        {{ return(tables) }}
    {% endif %}
{% endmacro %}


{# list specs columns from table #}
{% macro list_specs_columns(table, schema) %}
    {% if execute %}
        {% set columns = [] %}
        {% set query %}
            select column_name
            from information_schema.columns
            where table_schema='{{schema}}' and table_name='{{table}}'
        {% endset %}  
        
        {% set res = run_query(query) %}

        {% for i in res.columns[0].values() %}
            {% if i not in ['sku','name','price','brand','category','rating','reviews_count','url',
                            'release_year','release_month','updated_at','partition_date'] %}
                {% do columns.append(i) %}
            {% endif %}
        {% endfor %}    

        {{ return(columns) }}
    {% endif %}
{% endmacro %}


{# get specs in dim_specs by category #}
{% macro build_specs_query(category, alias='b') %}
    {% if execute %}
        {% set ctg = [] %} 

        {% if category|lower=='phone' %} {# grouping categories #}
            {% set ctg = ['Smartphone', 'Phone'] %}
        {% elif category|lower=='laptop' %}
            {% set ctg = ['Laptop'] %}
        {% elif category|lower=='tablet' %}
            {% set ctg = ['Tablet'] %}
        {% elif category|lower=='watch' %}
            {% set ctg = ['Smartwatch', 'Smartband'] %}
        {% elif category|lower=='earphones' %}
            {% set ctg = ['Earphones', 'Headphone', 'Earbuds'] %}
        {% elif category|lower=='screen' %}
            {% set ctg = ['Smartphone', 'Phone'] %}
        {% endif %}

        {% set query %} {# get list of columns names and datatypes #}
            select b.specs_name specs,
                b.data_type type
            from {{ ref('dim_product') }} a
                left join {{ ref('dim_specs') }} b on a.sku = b.product_sku
            where a.category in {{"('"~ctg|join("', '")~"')"}}
            group by b.specs_name, b.data_type
            order by b.specs_name
        {% endset %}
        {% set res = run_query(query) %}

        {% set final_query %}
            {%- for i in res.rows -%}
                {% set type = i[1] %}

                {%- if i[1]!='varchar' -%}
                    {% set type = 'double' %}
                {%- endif %}

                max(case
                    when {{alias}}.specs_name = '{{i[0]}}' then try_cast({{alias}}.specs_value as {{type}})
                end) {{i[0]}}

                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        {% endset %}

        {{ return(final_query) }}
    {% endif %}
{% endmacro %}
