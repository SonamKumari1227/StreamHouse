{#
  Open each key's FIRST version backwards to the beginning of time.

  An SCD2 dimension only knows history from the moment CDC started capturing it. Facts older
  than that - a backfill, a reload after a topic expired, or simply an order placed before the
  dimension's first observed change - match no version at all, and a point-in-time join
  silently returns null for every one of them. Nulls that mean "we started watching late" are
  indistinguishable from nulls that mean "no such restaurant", which is the dangerous part.

  So the earliest version of each key is treated as having been in force forever. The true
  `valid_from` is kept alongside, so the distinction is still available to anyone who needs it.

  This is a presentation decision in Gold, deliberately not made in Silver: Silver records what
  was observed, and inventing a window there would be a lie about the source.
#}

{% macro scd2_effective_from(key, valid_from='valid_from') %}
    case
        when row_number() over (partition by {{ key }} order by {{ valid_from }}) = 1
            then timestamp '1900-01-01 00:00:00'
        else {{ valid_from }}
    end
{% endmacro %}


{#
  The point-in-time join predicate: the version of `alias` in force at `at_ts`.
  Half-open [effective_from, valid_to) so exactly one version matches any instant.
#}
{% macro scd2_as_of(alias, at_ts) %}
    {{ at_ts }} >= {{ alias }}.effective_from
    and ({{ alias }}.valid_to is null or {{ at_ts }} < {{ alias }}.valid_to)
{% endmacro %}
