{#
  Use the custom schema as given, rather than appending it to the target schema.

  dbt's default would build `gold.dim_date` as `gold_gold.dim_date` - the target schema and
  the model's custom schema concatenated. That default exists so several developers can share
  a warehouse without colliding, which is not this situation: the whole thing runs locally
  against a Derby metastore nobody else touches.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
