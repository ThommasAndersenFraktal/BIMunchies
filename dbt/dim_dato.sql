with
    dato as (
        {{
            generate_date_table(
                "1970-01-01",
                (modules.datetime.datetime.now().year + 20) | string ~ "-12-31",
            )
        }}
    ),

    transform as (
        select
            --Dato
            dato_pk as dato_id,
            dato,
            uke_start_dato,
            uke_slutt_dato,
            maaned_start_dato,
            maaned_slutt_dato,
            kvartal_start_dato,
            kvartal_slutt_dato,

            --Årstall
            aar_for_uke,
            aar,

            --Nummer
            maaned_nummer,
            aar_maaned_nummer,
            maaned_aar_nummer,
            dag_i_maaned_nummer,
            ukedag_nummer,
            uke_nummer,
            uke_navn,
            aar_uke_nummer,
            aar_kvartal_nummer,

            --Navn
            ukedag_navn_kort,
            ukedag_navn_lang,
            uke_aar_navn,
            maaned_navn_kort,
            maaned_navn_lang,
            maaned_aar_navn,
            aar_maaned_navn,
            kvartal_navn,
            kvartal_aar_navn,

            --Logikk
            er_arbeidsdag,
            er_til_og_med_i_dag,
            er_til_og_med_i_gaar,

            --Antall passert
            antall_passerte_dager,
            antall_passerte_aar,
            antall_passerte_maaneder,
            antall_passerte_kvartal,
            antall_passerte_uker,

            --Dynamisk
            dag_dynamisk,
            uke_dynamisk,
            maaned_aar_dynamisk,
            aar_dynamisk
        from
            dato
    )

select *
from transform
