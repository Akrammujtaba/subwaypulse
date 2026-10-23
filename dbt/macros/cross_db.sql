{#- The marts run on Trino; CI runs them on DuckDB. These cover the few functions that differ. -#}

{#- A timestamptz as a plain New York local timestamp. Service hours are local. -#}
{% macro to_nyc(col) -%}
    {%- if target.type == 'trino' -%}
        cast(at_timezone({{ col }}, 'America/New_York') as timestamp(6))
    {%- else -%}
        timezone('America/New_York', {{ col }})
    {%- endif -%}
{%- endmacro %}

{% macro median(col) -%}
    {%- if target.type == 'trino' -%}
        approx_percentile({{ col }}, 0.5)
    {%- else -%}
        quantile_cont({{ col }}, 0.5)
    {%- endif -%}
{%- endmacro %}
