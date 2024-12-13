{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch('date_part', 'dbt_date') (datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro trino__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro clickhouse__date_part(datepart, date) -%}
    {%- if datepart == 'dayofweek' -%}
      toDayOfWeek({{ date }}, 2) 
    {%- elif datepart == 'isoweek' -%}
      toISOWeek({{ date }}) 
    {%- elif datepart == 'month' -%}
      toMonth({{ date }}) 
    {%- elif datepart == 'dayofyear' -%}
      toDayOfYear({{ date }}) 
    {%- elif datepart == 'day' or datepart == 'dayofmonth' -%}
      toDayOfMonth({{ date }}) 
    {%- elif datepart == 'epoch' -%}
      toUTCTimestamp({{ date }}, 'UTC')
    {%- elif datepart == 'week' -%}
      toWeek({{ date }})
    {%- else -%}
    dateName('{{ datepart }}', {{ date }})
    {%- endif -%}
{%- endmacro %}
