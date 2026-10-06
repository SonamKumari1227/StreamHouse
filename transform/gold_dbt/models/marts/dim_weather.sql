{#
  Grain: one row per (city, day).

  From Open-Meteo via ingestion/reference_data.py. If that API was unreachable the table is
  empty, every fact's weather_sk is null, and the weather aggregates return nothing - which is
  the correct behaviour for a measure whose input is missing, and far better than a zero that
  reads as "it did not rain".

  `is_rainy` at 1mm: below that is drizzle that changes no rider's behaviour, and the point of
  the flag is to split days into ones that plausibly affect transit time and ones that do not.
#}

select
    {{ surrogate_key(['city', 'weather_date']) }}    as weather_sk,
    city,
    weather_date,
    precipitation_mm,
    temp_max_c,
    temp_min_c,
    coalesce(precipitation_mm, 0) >= 1.0             as is_rainy,
    case
        when precipitation_mm is null then 'UNKNOWN'
        when precipitation_mm >= 10.0 then 'HEAVY_RAIN'
        when precipitation_mm >= 1.0 then 'RAIN'
        else 'CLEAR'
    end                                              as weather_band
from {{ source('bronze', 'ref_weather') }}
