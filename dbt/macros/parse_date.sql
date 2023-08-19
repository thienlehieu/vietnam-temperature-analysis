{% macro bigquery_to_date(expression) -%}
    parse_date("%Y%m%d", {{ expression }})
{%- endmacro %}