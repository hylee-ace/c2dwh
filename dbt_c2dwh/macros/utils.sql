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
