with
    dato as (
        {{
            generate_date_table(
                "2018-01-01",
                (modules.datetime.datetime.now().year + 1) | string ~ "-12-31",
            )
        }}
    ),
    delte_dimensjoner_seed as (
        select * from {{ ref("seed_delte_dimensjoner__oversettelse") }}
    ),

    transform as (
        select
            dato_pk as dato_id,
            dato,
            maaned_nummer,
            coalesce(
                translated_maaned_navn_kort.model_value_translated,
                dato.maaned_navn_kort
            ) as maaned_navn_kort,
            coalesce(
                translated_maaned_navn_lang.model_value_translated,
                dato.maaned_navn_lang
            ) as maaned_navn_lang,
            maaned_start_dato,
            maaned_slutt_dato,
            aar_maaned_nummer,
            maaned_aar_nummer,
            dag_i_maaned_nummer,
            aar,
            ukedag_nummer,
            coalesce(
                translated_ukedag_navn.model_value_translated, dato.ukedag_navn
            ) as ukedag_navn,
            uke_nummer,
            uke_navn,
            aar_for_uke,
            aar_uke_nummer,
            aar_kvartal_nummer,
            uke_aar_navn,
            lower(aar_maaned_navn) as aar_maaned_navn,
            maaned_aar_navn,
            uke_start_dato,
            uke_slutt_dato,
            er_arbeidsdag,
            kvartal_start_dato,
            kvartal_slutt_dato,
            kvartal_navn,
            kvartal_aar_navn,
            er_til_og_med_i_dag,
            er_til_og_med_i_gaar,
            antall_passerte_dager,
            antall_passerte_aar,
            antall_passerte_maaneder,
            antall_passerte_kvartal,
            antall_passerte_uker,
            coalesce(
                translated_dag_dynamisk.model_value_translated, dato.dag_dynamisk
            ) as dag_dynamisk,
            uke_dynamisk,
            replace(
                coalesce(
                    translated_maaned_aar_dynamisk.model_value_translated,
                    dato.maaned_aar_dynamisk
                ),
                dato.maaned_navn_kort,
                translated_maaned_navn_kort.model_value_translated
            ) as maaned_aar_dynamisk,
            coalesce(
                translated_aar_dynamisk.model_value_translated, dato.aar_dynamisk
            ) as aar_dynamisk
        from dato
        left outer join
            delte_dimensjoner_seed as translated_maaned_navn_kort
            on trim(translated_maaned_navn_kort.model_value_original)
            = trim(dato.maaned_navn_kort)
            and translated_maaned_navn_kort.model_table = 'dato_dimensjon'
            and translated_maaned_navn_kort.model_field = 'maaned_navn_kort'
        left outer join
            delte_dimensjoner_seed as translated_maaned_navn_lang
            on trim(translated_maaned_navn_lang.model_value_original)
            = trim(dato.maaned_navn_lang)
            and translated_maaned_navn_lang.model_table = 'dato_dimensjon'
            and translated_maaned_navn_lang.model_field = 'maaned_navn_lang'
        left outer join
            delte_dimensjoner_seed as translated_ukedag_navn
            on trim(translated_ukedag_navn.model_value_original)
            = trim(dato.ukedag_navn)
            and translated_ukedag_navn.model_table = 'dato_dimensjon'
            and translated_ukedag_navn.model_field = 'ukedag_navn'
        left outer join
            delte_dimensjoner_seed as translated_maaned_aar_dynamisk
            on trim(translated_maaned_aar_dynamisk.model_value_original)
            = trim(dato.maaned_aar_dynamisk)
            and translated_maaned_aar_dynamisk.model_table = 'dato_dimensjon'
            and translated_maaned_aar_dynamisk.model_field = 'maaned_aar_dynamisk'
        left outer join
            delte_dimensjoner_seed as translated_aar_dynamisk
            on trim(translated_aar_dynamisk.model_value_original)
            = trim(dato.aar_dynamisk)
            and translated_aar_dynamisk.model_table = 'dato_dimensjon'
            and translated_aar_dynamisk.model_field = 'aar_dynamisk'
        left outer join
            delte_dimensjoner_seed as translated_dag_dynamisk
            on trim(translated_dag_dynamisk.model_value_original)
            = trim(dato.maaned_aar_dynamisk)
            and translated_dag_dynamisk.model_table = 'dato_dimensjon'
            and translated_dag_dynamisk.model_field = 'dag_dynamisk'
    )

select *
from transform
