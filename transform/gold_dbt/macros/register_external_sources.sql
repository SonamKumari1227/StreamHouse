{#
  Make the path-based Delta tables visible to the catalog.

  Silver and Bronze are written by Spark jobs straight to s3a:// paths - deliberately, because
  the streaming jobs should not need a metastore to do their work. dbt's `source()` needs a
  named relation, so this registers each path as an external table once, before any model
  runs. CREATE TABLE IF NOT EXISTS means a re-run costs nothing.

  External, never managed: `DROP TABLE` here would remove the catalog entry and leave the data
  where it is. The Spark jobs own these files; dbt only reads them.
#}

{% macro register_external_sources() %}
  {% set sources = [
      ('silver', 'fact_order_state',      's3a://silver/fact_order_state'),
      ('silver', 'dim_restaurant_scd2',   's3a://silver/dim_restaurant_scd2'),
      ('silver', 'dim_rider_scd2',        's3a://silver/dim_rider_scd2'),
      ('silver', 'dim_menu_item_scd2',    's3a://silver/dim_menu_item_scd2'),
      ('silver', 'gps_trips_sessionized', 's3a://silver/gps_trips_sessionized'),
      ('bronze', 'raw_customers_cdc',     's3a://bronze/raw_customers_cdc'),
      ('bronze', 'raw_order_items_cdc',   's3a://bronze/raw_order_items_cdc'),
      ('bronze', 'ref_holidays',          's3a://bronze/ref_holidays'),
      ('bronze', 'ref_weather',           's3a://bronze/ref_weather'),
  ] %}

  {% for schema, table, location in sources %}
    {% do run_query('CREATE DATABASE IF NOT EXISTS ' ~ schema) %}
    {% do run_query(
         'CREATE TABLE IF NOT EXISTS ' ~ schema ~ '.' ~ table ~
         " USING DELTA LOCATION '" ~ location ~ "'"
       ) %}
  {% endfor %}

  {% do log('registered ' ~ sources | length ~ ' external source table(s)', info=true) %}
{% endmacro %}
