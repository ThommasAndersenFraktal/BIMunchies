{% macro generate_date_table(start_date, end_date) %}

    {# Convert the string dates into datetime objects #}
    {% set start_date_obj = modules.datetime.datetime.strptime(
        start_date, "%Y-%m-%d"
    ) %}
    {% set end_date_obj = modules.datetime.datetime.strptime(end_date, "%Y-%m-%d") %}
    {% set days_between = (end_date_obj - start_date_obj).days %}

    with
        date_series as (
            select cast(dato as date) as dato
            from
                (
                    values
                        {% for i in range(days_between + 1) %}
                            (dateadd(day, {{ i }}, cast('{{ start_date }}' as date)))
                            {% if not loop.last %},{% endif %}
                        {% endfor %}
                ) as date_series(dato)
        )

    select
        cast(dato as string) as dato_pk,
        dato,
        month(dato) as maaned_nummer,
        date_format(dato, 'MMM') as maaned_navn_kort,
        date_format(dato, 'MMMM') as maaned_navn_lang,
        cast(date_trunc('month', dato) as date) as maaned_start_dato,
        last_day(dato) as maaned_slutt_dato,
        concat(
            year(dato), lpad(cast(month(dato) as string), 2, '0')
        ) as aar_maaned_nummer,
        concat(date_format(dato, 'MMM'), ' ', year(dato)) as maaned_aar_nummer,
        day(dato) as dag_i_maaned_nummer,
        year(dato) as aar,
        (((dayofweek(dato) + 5) % 7) + 1) as ukedag_nummer,
        date_format(dato, 'E') as ukedag_navn,
        weekofyear(dato) as uke_nummer,
        concat_ws(
            ' ', 'Uke', lpad(cast(weekofyear(dato) as string), 2, '0')
        ) as uke_navn,
        year(date_add(dato, 26 - weekofyear(dato))) as aar_for_uke,
        concat(
            year(date_add(dato, 26 - weekofyear(dato))),
            lpad(cast(weekofyear(dato) as string), 2, '0')
        ) as aar_uke_nummer,
        concat(year(dato), quarter(dato)) as aar_kvartal_nummer,
        concat_ws(
            ' ',
            'Uke',
            concat_ws(
                ' - ', cast(weekofyear(dato) as string), cast(year(dato) as string)
            )
        ) as uke_aar_navn,
        concat(year(dato), ' ', date_format(dato, 'MMM')) as aar_maaned_navn,
        concat(
            upper(substring(date_format(dato, 'MMM'), 1, 1)),
            substring(date_format(dato, 'MMM'), 2, length(date_format(dato, 'MMM'))),
            ' ',
            cast(year(dato) as string)
        ) as maaned_aar_navn,
        cast(date_trunc('week', dato) as date) as uke_start_dato,
        date_add(date_trunc('week', dato), 6) as uke_slutt_dato,
        case
            when (((dayofweek(dato) + 5) % 7) + 1) < 6 then 1 else 0
        end as er_arbeidsdag,
        trunc(dato, 'quarter') as kvartal_start_dato,
        date_sub(add_months(trunc(dato, 'quarter'), 3), 1) as kvartal_slutt_dato,
        concat(quarter(dato), '. kvartal') as kvartal_navn,
        concat(
            concat(quarter(dato), '. kvartal '), cast(year(dato) as string)
        ) as kvartal_aar_navn,
        case when dato <= current_date() then 1 else 0 end as er_til_og_med_i_dag,
        case when dato < current_date() then 1 else 0 end as er_til_og_med_i_gaar,
        datediff(current_date(), dato) as antall_passerte_dager,
        year(current_date()) - year(dato) as antall_passerte_aar,
        (
            (year(current_date()) * 12 + month(current_date()))
            - (year(dato) * 12 + month(dato))
        ) as antall_passerte_maaneder,
        (
            (year(current_date()) * 4 + quarter(current_date()))
            - (year(dato) * 4 + quarter(dato))
        ) as antall_passerte_kvartal,
        (
            (year(current_date()) * 53 + weekofyear(current_date()))
            - (year(dato) * 53 + weekofyear(dato))
        ) as antall_passerte_uker,
        case
            when datediff(current_date(), dato) = 0
            then 'I dag'
            when datediff(current_date(), dato) = 1
            then 'I gaar'
            when datediff(current_date(), dato) = -1
            then 'I morgen'
            else date_format(dato, 'yyyy-MM-dd')
        end as dag_dynamisk,
        case
            when
                weekofyear(dato) = weekofyear(current_date())
                and year(dato) = year(current_date())
            then 'Denne uke'
            when
                weekofyear(dato) = weekofyear(current_date()) - 1
                and year(dato) = year(current_date())
            then 'Forrige uke'
            when
                weekofyear(dato) = weekofyear(current_date()) + 1
                and year(dato) = year(current_date())
            then 'Neste uke'
            else
                concat(
                    'Uke ',
                    lpad(cast(weekofyear(dato) as string), 2, '0'),
                    ' ',
                    cast(year(dato) as string)
                )
        end as uke_dynamisk,
        case
            when
                (
                    (year(dato) * 12 + month(dato))
                    - (year(current_date()) * 12 + month(current_date()))
                )
                = 0
            then 'Denne maaned'
            when
                (
                    (year(dato) * 12 + month(dato))
                    - (year(current_date()) * 12 + month(current_date()))
                )
                = -1
            then 'Forrige maaned'
            when
                (
                    (year(dato) * 12 + month(dato))
                    - (year(current_date()) * 12 + month(current_date()))
                )
                = 1
            then 'Neste maaned'
            else concat(substr(date_format(dato, 'MMMM'), 1, 3), ' ', year(dato))
        end as maaned_aar_dynamisk,
        case
            when year(current_date()) - year(dato) = 0
            then 'Dette aar'
            when year(current_date()) - year(dato) = 1
            then 'Forrige aar'
            when year(current_date()) - year(dato) = -1
            then 'Neste aar'
            else cast(year(dato) as string)
        end as aar_dynamisk
    from date_series
    order by dato

{% endmacro %}
