let
    Source = {
        Number.From(
            #date(
                Date.Year(DateTime.LocalNow()) - 1,
                01,
                01
            )
        )
        ..
        Number.From(
            #date(
                Date.Year(DateTime.LocalNow()),
                12,
                31
            )
        )
    },
    Convert =
        Table.FromList(
            Source,
            Splitter.SplitByNothing(),
            null,
            null,
            ExtraValues.Error
        ),
    Step1 =
        Table.TransformColumnTypes(
            Convert,
            {
                {
                    "Column1",
                    type date
                }
            }
        ),
    Step2 =
        Table.RenameColumns(
            Step1,
            {
                {
                    "Column1",
                    "Dato"
                }
            }
        ),
    Step3 =
        Table.AddColumn(
            Step2,
            "#Uke",
            each Date.WeekOfYear([Dato])
        ),
    Step3b =
        Table.AddColumn(
            Step3,
            "#Uke-ISO",
            each
                if
                    Number.RoundDown(
                        (
                            Date.DayOfYear([Dato])
                            - (Date.DayOfWeek([Dato], Day.Monday) + 1)
                            + 10
                        )
                        / 7
                    )
                    = 0
                then
                    Number.RoundDown(
                        (
                            Date.DayOfYear(#date(Date.Year([Dato]) - 1, 12, 31))
                            - (
                                Date.DayOfWeek(
                                    #date(Date.Year([Dato]) - 1, 12, 31),
                                    Day.Monday
                                )
                                + 1
                            )
                            + 10
                        )
                        / 7
                    )
                else if
                    (
                        Number.RoundDown(
                            (
                                Date.DayOfYear([Dato])
                                - (Date.DayOfWeek([Dato], Day.Monday) + 1)
                                + 10
                            )
                            / 7
                        )
                        = 53 and (
                            Date.DayOfWeek(
                                #date(Date.Year([Dato]), 12, 31),
                                Day.Monday
                            )
                            + 1
                            < 4
                        )
                    )
                then
                    1
                else
                    Number.RoundDown(
                        (
                            Date.DayOfYear([Dato])
                            - (Date.DayOfWeek([Dato], Day.Monday) + 1)
                            + 10
                        )
                        / 7
                    )
        ),
    Step4 =
        Table.AddColumn(
            Step3b,
            "#År",
            each Date.Year([Dato])
        ),
    Step4b =
        Table.AddColumn(
            Step4,
            "#År-ISO",
            each Date.Year(Date.AddDays([Dato], 26 - [#"#Uke-ISO"]))
        ),
    Step5 =
        Table.AddColumn(
            Step4b,
            "#Kvartal",
            each Date.QuarterOfYear([Dato])
        ),
    Step6 =
        Table.AddColumn(
            Step5,
            "#Måned",
            each Date.Month([Dato])
        ),
    Step7 =
        Table.AddColumn(
            Step6,
            "#Dag (år)",
            each Date.DayOfYear([Dato])
        ),
    Step8 =
        Table.AddColumn(
            Step7,
            "#Dag (uke)",
            each Date.DayOfWeek([Dato], 1) + 1
        ),
    Step9 =
        Table.AddColumn(
            Step8,
            "#Dag (måned)",
            each Date.Day([Dato])
        ),
    Step10 =
        Table.AddColumn(
            Step9,
            "#År og #kvartal",
            each
                Text.From([#"#År"])
                & Text.End(
                    "0" & Text.From([#"#Kvartal"]),
                    2
                )
        ),
    Step11 =
        Table.AddColumn(
            Step10,
            "#År og #måned",
            each
                Text.From([#"#År"])
                & Date.ToText([Dato], "MM")
        ),
    Step12 =
        Table.AddColumn(
            Step11,
            "#År og #uke",
            each
                Text.From([#"#År-ISO"])
                & Text.End(
                    "0" & Text.From([#"#Uke-ISO"]),
                    2
                )
        ),
    Step12b =
        Table.AddColumn(
            Step12,
            "#År-ISO og #uke-ISO",
            each
                Text.From([#"#År-ISO"])
                & Text.End(
                    "0" & Text.From([#"#Uke-ISO"]),
                    2
                )
        ),
    Step13 =
        Table.AddColumn(
            Step12b,
            "Ukedag",
            each
                Text.Start(
                    Text.Proper(Date.DayOfWeekName([Dato])),
                    3
                )
        ),
    Step14 =
        Table.AddColumn(
            Step13,
            "Måned kort",
            each
                Text.Start(
                    Text.Proper(Date.MonthName([Dato])),
                    3
                )
        ),
    Step15 =
        Table.AddColumn(
            Step14,
            "Måned kort og år",
            each
                Text.From([Måned kort])
                & " "
                & Text.From([#"#År"])
        ),
    Step16 =
        Table.AddColumn(
            Step15,
            "Måned",
            each Text.Proper(Date.MonthName([Dato]))
        ),
    Step17 =
        Table.AddColumn(
            Step16,
            "Måned og år",
            each
                Text.From([Måned])
                & " "
                & Text.From([#"#År"])
        ),
    Step18 =
        Table.AddColumn(
            Step17,
            "Kvartal og år",
            each
                Text.From("K")
                & Text.End(
                    "0" & Text.From([#"#Kvartal"]),
                    2
                )
                & " "
                & Text.From([#"#År"])
        ),
    Step19 =
        Table.AddColumn(
            Step18,
            "Uke og år",
            each
                "U"
                & Text.End(
                    "0" & Text.From([#"#Uke-ISO"]),
                    2
                )
                & " "
                & Text.From([#"#År-ISO"])
        ),
    Step19b =
        Table.AddColumn(
            Step19,
            "Uke-ISO og år-ISO",
            each
                "U"
                & Text.End(
                    "0" & Text.From([#"#Uke-ISO"]),
                    2
                )
                & " "
                & Text.From([#"#År-ISO"])
        ),
    Step20 =
        Table.AddColumn(
            Step19b,
            "Dato  siste i måned",
            each Date.EndOfMonth([Dato])
        ),
    Step21 =
        Table.AddColumn(
            Step20,
            "Inntil i dag",
            each
                if [Dato] <= Date.From(DateTime.LocalNow()) then
                    1
                else
                    0
        ),
    Step22 =
        Table.AddColumn(
            Step21,
            "Inntil i går",
            each
                if [Dato] < Date.From(DateTime.LocalNow()) then
                    1
                else
                    0
        ),
    Step22b =
        Table.AddColumn(
            Step22,
            "Inntil forrige måned",
            each
                if                 Duration.Days(
                    Duration.From(
                        Date.From(DateTime.LocalNow())
                        - Date.From([Dato])
                    )
                ) > 0 then
                    1
                else
                    0
        ),
    Step23 =
        Table.AddColumn(
            Step22b,
            "#Passerte dager",
            each
                Duration.Days(
                    Duration.From(
                        Date.From(DateTime.LocalNow())
                        - Date.From([Dato])
                    )
                )
        ),
    Step24 =
        Table.AddColumn(
            Step23,
            "#Passerte år",
            each
                Value.FromText(Date.Year(Date.From(DateTime.LocalNow())))
                - Value.FromText([#"#År"])
        ),
    Step25 =
        Table.AddColumn(
            Step24,
            "#Passerte måneder",
            each
                ([#"#Passerte år"] * 12)
                + (
                    Value.FromText(Text.From(Date.Month(Date.From(DateTime.LocalNow()))))
                    - Value.FromText([#"#Måned"])
                )
        ),
    Step26 =
        Table.AddColumn(
            Step25,
            "#Passerte kvartal",
            each
                ([#"#Passerte år"] * 4)
                + (
                    Value.FromText(Text.From(Date.QuarterOfYear(Date.From(DateTime.LocalNow()))))
                    - Value.FromText([#"#Kvartal"])
                )
        ),
    Step27 =
        Table.AddColumn(
            Step26,
            "#Passerte uker",
            each
                Duration.Days(
                    Duration.From(
                        Date.From(
                            Date.StartOfWeek(
                                Date.From(DateTime.LocalNow()),
                                1
                            )
                        )
                        - Date.From(Date.StartOfWeek(Date.From([Dato]), 1))
                    )
                )
                / 7
        ),
    Step28 =
        Table.AddColumn(
            Step27,
            "Filter dag",
            each
                if [#"#Passerte dager"] = 0 then
                    "Inneværende dag"
                else if [#"#Passerte dager"] = 1 then
                    "Forrige dag"
                else if [#"#Passerte dager"] = -1 then
                    "Neste dag"
                else
                    [Dato]
        ),
    Step29 =
        Table.AddColumn(
            Step28,
            "Filter måned",
            each
                if [#"#Passerte måneder"] = 0 then
                    "Inneværende måned"
                else if [#"#Passerte måneder"] = 1 then
                    "Forrige måned"
                else if [#"#Passerte måneder"] = -1 then
                    "Neste måned"
                else
                    [Måned kort og år]
        ),
    Step29b =
        Table.AddColumn(
            Step29,
            "Filter uke",
            each
                if [#"#Passerte uker"] = 0 then
                    "Inneværende uke"
                else if [#"#Passerte uker"] = 1 then
                    "Forrige uke"
                else if [#"#Passerte uker"] = -1 then
                    "Neste uke"
                else
                    [Uke og år]
        ),
    Step30 =
        Table.AddColumn(
            Step29b,
            "Filter år",
            each
                if [#"#Passerte år"] = 0 then
                    "Inneværende år"
                else if [#"#Passerte år"] = 1 then
                    "Forrige år"
                else if [#"#Passerte år"] = -1 then
                    "Neste år"
                else
                    [#"#År"]
        ),
    Step31 =
        Table.TransformColumnTypes(
            Step30,
            {
                {
                    "#Passerte uker",
                    Int64.Type
                },
                {
                    "#Passerte kvartal",
                    Int64.Type
                },
                {
                    "#Passerte måneder",
                    Int64.Type
                },
                {
                    "#Passerte år",
                    Int64.Type
                },
                {
                    "#Passerte dager",
                    Int64.Type
                },
                {
                    "#År",
                    Int64.Type
                },
                {
                    "#Kvartal",
                    Int64.Type
                },
                {
                    "#Måned",
                    Int64.Type
                },
                {
                    "#Uke",
                    Int64.Type
                },
                {
                    "#Dag (uke)",
                    Int64.Type
                },
                {
                    "#Dag (år)",
                    Int64.Type
                },
                {
                    "#Dag (måned)",
                    Int64.Type
                },
                {
                    "#År og #kvartal",
                    Int64.Type
                },
                {
                    "#År og #måned",
                    Int64.Type
                },
                {
                    "#År og #uke",
                    Int64.Type
                },
                {
                    "Inntil i dag",
                    type logical
                },
                {
                    "Inntil i går",
                    type logical
                },
                {
                    "Dato  siste i måned",
                    type date
                },
                {
                    "Dato",
                    type date
                },
                {
                    "Ukedag",
                    type text
                },
                {
                    "Måned kort",
                    type text
                },
                {
                    "Måned kort og år",
                    type text
                },
                {
                    "Måned",
                    type text
                },
                {
                    "Måned og år",
                    type text
                },
                {
                    "Kvartal og år",
                    type text
                },
                {
                    "Uke og år",
                    type text
                },
                {
                    "Filter dag",
                    type text
                },
                {
                    "Filter måned",
                    type text
                },
                {
                    "Filter år",
                    type text
                },
                {
                    "#År-ISO",
                    Int64.Type
                },
                {
                    "#Uke-ISO",
                    Int64.Type
                },
                {
                    "#År-ISO og #uke-ISO",
                    Int64.Type
                },
                {
                    "Uke-ISO og år-ISO",
                    type text
                }
            }
        )
in
    Step31
