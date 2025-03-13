{% macro generate_date_table(start_date, end_date) %}

WITH 
  -- 1) Generate all dates from start_date to end_date
  date_series AS (
    SELECT
      explode(
        sequence(
          to_date('{{ start_date }}'),
          to_date('{{ end_date }}'),
          interval 1 day
        )
      ) AS dato
  ),
        maaned_navn as (
            select
                maaned_nummer,
                maaned_navn_kort,
                maaned_navn_lang
            from
                (
                    values
                        (1,'Jan','Januar'),
                        (2, 'Feb', 'Februar'),
                        (3, 'Mar', 'Mars'),
                        (4, 'Apr', 'April'),
                        (5, 'Maj', 'Mai'),
                        (6, 'Jun', 'Juni'),
                        (7, 'Jul', 'Juli'),
                        (8, 'Aug', 'August'),
                        (9, 'Sep', 'September'),
                        (10, 'Okt', 'Oktober'),
                        (11, 'Nov', 'November'),
                        (12, 'Des', 'Desember')
                            )
            as
                maaned_navn(
                    maaned_nummer,
                    maaned_navn_kort,
                    maaned_navn_lang
                )          

        ),
        ukedag_navn as (
            select
                ukedag_nummer,
                ukedag_navn_kort,
                ukedag_navn_lang
            from
                (
                values
                    (1, 'Man', 'Mandag'),
                    (2, 'Tir', 'Tirsdag'),
                    (3, 'Ons', 'Onsdag'),
                    (4, 'Tor', 'Torsdag'),
                    (5, 'Fre', 'Fredag'),
                    (6, 'Lør', 'Lørdag'),
                    (7, 'Søn', 'Søndag')
                )
            as
                ukedag_navn(
                    ukedag_nummer,
                    ukedag_navn_kort,
                    ukedag_navn_lang    
                )

        ),
        result as (
            select
                cast(dato as string)                                                            as dato_pk,
                dato,
                month(dato)                                                                     as maaned_nummer,

                cast(date_trunc('month', dato) as date)                                         as maaned_start_dato,
                last_day(dato)                                                                  as maaned_slutt_dato,
                concat(
                    year(dato), lpad(cast(month(dato) as string), 2, '0')
                )                                                                               as aar_maaned_nummer,
                concat(date_format(dato, 'MMM'), ' ', year(dato))                               as maaned_aar_nummer,
                day(dato)                                                                       as dag_i_maaned_nummer,
                year(dato)                                                                      as aar,
                (((dayofweek(dato) + 5) % 7) + 1)                                               as ukedag_nummer,

                weekofyear(dato)                                                                as uke_nummer,
                concat_ws(
                    ' ', 'Uke', lpad(cast(weekofyear(dato) as string), 2, '0')
                )                                                                               as uke_navn,
                year(date_add(dato, 26 - weekofyear(dato)))                                     as aar_for_uke,
                concat(
                    year(date_add(dato, 26 - weekofyear(dato))),
                    lpad(cast(weekofyear(dato) as string), 2, '0')
                )                                                                               as aar_uke_nummer,
                concat(year(dato), quarter(dato))                                               as aar_kvartal_nummer,
                concat_ws(
                    ' ',
                    'Uke',
                    concat_ws(
                        ' - ', cast(weekofyear(dato) as string), cast(year(dato) as string)
                    )
                )                                                                               as uke_aar_navn,

                ukedag_navn.ukedag_navn_kort                                                    as ukedag_navn_kort,
                ukedag_navn.ukedag_navn_lang    	                                            as ukedag_navn_lang,
                maaned_navn.maaned_navn_kort                                                    as maaned_navn_kort,
                maaned_navn.maaned_navn_lang                                                    as maaned_navn_lang,

                concat_ws(' ',year(dato),lower(ukedag_navn.ukedag_navn_kort))                   as aar_maaned_navn,
                concat_ws(' ',lower(ukedag_navn.ukedag_navn_kort),year(dato))                   as maaned_aar_navn,
                cast(date_trunc('week', dato) as date)                                          as uke_start_dato,
                date_add(date_trunc('week', dato), 6)                                           as uke_slutt_dato,
                case
                    when (((dayofweek(dato) + 5) % 7) + 1) < 6 then 1 else 0
                end                                                                             as er_arbeidsdag,
                trunc(dato, 'quarter')                                                          as kvartal_start_dato,
                date_sub(add_months(trunc(dato, 'quarter'), 3), 1)                              as kvartal_slutt_dato,
                concat(quarter(dato), '. kvartal')                                              as kvartal_navn,
                concat(
                    concat(quarter(dato), '. kvartal '), cast(year(dato) as string)
                )                                                                               as kvartal_aar_navn,
                case when dato <= current_date() then 1 else 0 end                              as er_til_og_med_i_dag,
                case when dato < current_date() then 1 else 0 end                               as er_til_og_med_i_gaar,
                datediff(current_date(), dato)                                                  as antall_passerte_dager,
                year(current_date()) - year(dato)                                               as antall_passerte_aar,
                (
                    (year(current_date()) * 12 + month(current_date()))
                    - (year(dato) * 12 + month(dato))
                )                                                                               as antall_passerte_maaneder,
                (
                    (year(current_date()) * 4 + quarter(current_date()))
                    - (year(dato) * 4 + quarter(dato))
                )                                                                               as antall_passerte_kvartal,
                (
                    (year(current_date()) * 53 + weekofyear(current_date()))
                    - (year(dato) * 53 + weekofyear(dato))
                )                                                                               as antall_passerte_uker,
                case
                    when datediff(current_date(), dato) = 0
                    then 'I dag'
                    when datediff(current_date(), dato) = 1
                    then 'I går'
                    when datediff(current_date(), dato) = -1
                    then 'I morgen'
                    else date_format(dato, 'yyyy-MM-dd')
                end                                                                             as dag_dynamisk,
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
                end                                                                             as uke_dynamisk,
                case
                    when
                        (
                            (year(dato) * 12 + month(dato))
                            - (year(current_date()) * 12 + month(current_date()))
                        )
                        = 0
                    then 'Denne måned'
                    when
                        (
                            (year(dato) * 12 + month(dato))
                            - (year(current_date()) * 12 + month(current_date()))
                        )
                        = -1
                    then 'Forrige måned'
                    when
                        (
                            (year(dato) * 12 + month(dato))
                            - (year(current_date()) * 12 + month(current_date()))
                        )
                        = 1
                    then 'Neste måned'
                    else concat_ws(' ',maaned_navn.maaned_navn_kort, year(dato))
                end                                                                             as maaned_aar_dynamisk,
                case
                    when year(current_date()) - year(dato) = 0
                    then 'Dette år'
                    when year(current_date()) - year(dato) = 1
                    then 'Forrige år'
                    when year(current_date()) - year(dato) = -1
                    then 'Neste år'
                    else cast(year(dato) as string)
                end                                                                             as aar_dynamisk
            from date_series
            left outer join
                maaned_navn
                on month(date_series.dato) = maaned_navn.maaned_nummer
            left outer join
                ukedag_navn
                on (((dayofweek(date_series.dato) + 5) % 7) + 1) = ukedag_navn.ukedag_nummer
        )

    select
        *
    from
        result

{% endmacro %}
