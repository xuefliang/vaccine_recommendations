import polars as pl
from typing import List, Tuple, Optional, Callable

def calculate_all_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
    # Vaccine schedule configuration
    VACCINE_SCHEDULE = [
        # (vaccine_name, dose, offset_from_birth, offset_from_prev, prev_dose, vaccine_category, condition)
        ('卡介苗', 1, '0d', None, None, '卡介苗', None),
        ('乙肝疫苗', 1, '0d', None, None, '乙肝疫苗', None),
        ('乙肝疫苗', 2, '1mo', '1mo', 1, '乙肝疫苗', None),
        ('乙肝疫苗', 3, None, None, None, '乙肝疫苗', lambda df: df.with_columns([
            ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗'))
            .any().over("id_x").alias("has_dose2")
        ]).with_columns([
            pl.when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") < 1) & 
                (pl.col("has_dose2"))
            ).then(pl.col("vaccination_date").dt.offset_by("6mo"))
            .when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") >= 1) & 
                (pl.col("has_dose2"))
            ).then(pl.col("vaccination_date").dt.offset_by("4mo"))
            .otherwise(pl.col("vaccination_date")).alias("from_dose1"),
            pl.when(
                (pl.col("vaccination_seq") == 2) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") < 1)
            ).then(pl.col("vaccination_date").dt.offset_by("1mo"))
            .when(
                (pl.col("vaccination_seq") == 2) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") >= 1)
            ).then(pl.col("vaccination_date").dt.offset_by("60d"))
            .otherwise(pl.col("vaccination_date")).alias("from_dose2")
        ]).with_columns(
            pl.max_horizontal([pl.col("from_dose1").shift(), pl.col("from_dose2")])
            .alias("recommended_dates")
        )),
        ('乙肝疫苗', 4, None, '5mo', 3, '乙肝疫苗', lambda df: df.with_columns(
            pl.when(
                ((pl.col("hepatitis_mothers") == '1') | 
                 ((pl.col("hepatitis_mothers") == '3') & (pl.col("birth_weight") < 2000))) & 
                (pl.col("vaccination_seq") == 3) & 
                (pl.col("vaccine_name") == '乙肝疫苗')
            ).then(pl.col("vaccination_date").dt.offset_by("5mo"))
            .otherwise(pl.col("vaccination_date")).alias("recommended_dates")
        )),
        ('脊灰疫苗', 1, '2mo', None, None, '脊灰疫苗', None),
        ('脊灰疫苗', 2, '3mo', '1mo', 1, '脊灰疫苗', None),
        ('脊灰疫苗', 3, '4mo', '1mo', 2, '脊灰疫苗', None),
        ('脊灰疫苗', 4, '4y', '1mo', 3, '脊灰疫苗', None),
        ('百白破疫苗', 1, '2mo', None, None, '百白破疫苗', None),
        ('百白破疫苗', 2, '4mo', '1mo', 1, '百白破疫苗', None),
        ('百白破疫苗', 3, '6mo', '1mo', 2, '百白破疫苗', None),
        ('百白破疫苗', 4, '18mo', '1mo', 3, '百白破疫苗', None),
        ('百白破疫苗', 5, '6y', '1mo', 4, '百白破疫苗', None),
        ('白破疫苗', 1, '7y', None, None, '白破疫苗', None),
        ('含麻疹成分疫苗', 1, '8mo', None, None, '含麻疹成分疫苗', None),
        ('含麻疹成分疫苗', 2, '18mo', '1mo', 1, '含麻疹成分疫苗', None),
        ('A群流脑疫苗', 1, '6mo', None, None, 'A群流脑疫苗', None),
        ('A群C群流脑疫苗', 2, '9mo', '3mo', 1, 'A群流脑疫苗', None),
        ('A群C群流脑疫苗', 1, None, None, None, 'A群C群流脑疫苗', lambda df: df.with_columns([
            ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2))
            .sum().over("id_x").alias("his_ma")
        ]).filter(
            ((pl.col('his_ma') == 2) & (pl.col("vaccine_name") == 'A群流脑疫苗')) |
            ((pl.col('his_ma') == 1) & (pl.col("vaccine_name") == 'A群流脑疫苗')) |
            (pl.col('his_ma') == 0)
        ).with_columns(
            pl.when(pl.col('his_ma') == 2)
            .then(pl.col("birth_date").dt.offset_by("3y"))
            .when(pl.col('his_ma') == 1)
            .then(pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("2y"),
                pl.col("vaccination_date").dt.offset_by("3mo")
            ]))
            .otherwise(pl.col("birth_date").dt.offset_by("2y"))
            .alias("recommended_dates")
        )),
        ('A群C群流脑疫苗', 2, '6y', '3y', 1, 'A群C群流脑疫苗', None),
        ('乙脑疫苗', 1, '8mo', None, None, '乙脑疫苗', None),
        ('乙脑疫苗', 2, '2y', '12mo', 1, '乙脑疫苗', None),
        ('甲肝疫苗', 1, '18mo', None, None, '甲肝疫苗', None),
        ('甲肝疫苗', 2, '2y', '6mo', 1, '甲肝疫苗', lambda df: df.with_columns(
            pl.when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("小类名称") == '甲型肝炎灭活疫苗（人二倍体细胞）')
            ).then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by("2y"),
                    pl.col("vaccination_date").dt.offset_by("6mo")
                ])
            ).otherwise(pl.col("vaccination_date")).alias("recommended_dates")
        ))
    ]

    def calculate_recommendation(
        df: pl.DataFrame, 
        vacc: str, 
        dose: int, 
        offset_birth: Optional[str], 
        offset_prev: Optional[str], 
        prev_dose: Optional[int], 
        category: str, 
        special_condition: Optional[Callable]
    ) -> pl.DataFrame:
        # Initialize columns
        df = df.with_columns([
            pl.lit(vacc).alias("recommended_vacc"),
            pl.lit(dose).alias("recommended_seq")
        ])

        # Apply special conditions if provided
        if special_condition:
            df = special_condition(df)
        else:
            df = df.with_columns(
                pl.when(
                    (prev_dose is not None) & 
                    (pl.col("vaccination_seq") == prev_dose) & 
                    (pl.col("vaccine_name") == vacc)
                ).then(
                    pl.max_horizontal([
                        pl.col("birth_date").dt.offset_by(offset_birth) if offset_birth else pl.col("birth_date"),
                        pl.col("vaccination_date").dt.offset_by(offset_prev) if offset_prev else pl.col("vaccination_date")
                    ])
                ).otherwise(
                    pl.col("birth_date").dt.offset_by(offset_birth) if offset_birth else pl.col("birth_date")
                ).alias("recommended_dates")
            )

        # Apply filtering logic
        df = df.with_columns(
            pl.when(
                (pl.col('recommended_seq') == dose) & 
                (pl.col("大类名称") == category) &
                (pl.col('vaccination_seq') == dose) &
                (pl.col('vaccination_date') > pl.col('recommended_dates'))
            ).then(pl.col('recommended_dates'))
            .when(
                (pl.col('recommended_seq') == dose) & 
                (
                    (pl.col("大类名称") == category) & 
                    (pl.col('vaccination_seq') == (prev_dose if prev_dose is not None else dose))
                ) | 
                (~pl.col("大类名称").is_in([category]))
            ).then(pl.col('recommended_dates'))
            .when(pl.col("recommended_dates").is_null())
            .then(pl.col("birth_date"))  # Fallback to birth_date if null
            .otherwise(pl.col("recommended_dates"))
            .alias("recommended_dates")
        )

        # Aggregate results
        return df.group_by("id_x").agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])

    # Calculate all recommendations
    recommendations = []
    for vacc, dose, offset_birth, offset_prev, prev_dose, category, condition in VACCINE_SCHEDULE:
        rec = calculate_recommendation(
            person, vacc, dose, offset_birth, offset_prev, prev_dose, category, condition
        )
        recommendations.append(rec)

    # Combine all recommendations
    return pl.concat(recommendations, how='vertical')

# Example usage
if __name__ == "__main__":
    vaccine_tbl = pl.read_excel('ym_bm.xlsx')
    person = (
        pl.read_csv(
            '/mnt/d/标准库接种率/data/person2.csv',
            schema_overrides={
                "vaccination_code": pl.Utf8,
                "birth_date": pl.Datetime,
                "vaccination_date": pl.Datetime,
                "hepatitis_mothers": pl.Utf8
            }
        )
        .with_columns([
            pl.lit('2021-01-15').str.to_date().dt.month_end().alias('expiration_date'),
            pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
            pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end')
        ])
        .with_columns([
            (
                pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year() -
                pl.when(
                    (pl.col("expiration_date").dt.month() < pl.col("birth_date").dt.month()) |
                    (
                        (pl.col("expiration_date").dt.month() == pl.col("birth_date").dt.month()) &
                        (pl.col("expiration_date").dt.day() < pl.col("birth_date").dt.day())
                    )
                ).then(1).otherwise(0)
            ).alias('age')
        ])
        .with_columns([
            (
                (pl.col("vaccination_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
                (pl.col("vaccination_date").dt.month() - pl.col("birth_date").dt.month()) +
                pl.when(pl.col("vaccination_date").dt.day() >= pl.col("birth_date").dt.day())
                .then(0).otherwise(-1)
            ).alias('vacc_month')
        ])
        .with_columns([
            (
                (pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
                (pl.col("expiration_date").dt.month() - pl.col("birth_date").dt.month()) +
                pl.when(pl.col("expiration_date").dt.day() >= pl.col("birth_date").dt.day())
                .then(0).otherwise(-1)
            ).alias('age_month')
        ])
        .join(vaccine_tbl, left_on="vaccination_code", right_on="小类编码", how='left')
        .with_columns(pl.col("vaccine_name").str.split(","))
        .explode("vaccine_name")
        .sort(["id_x", "vaccine_name", "vaccination_date"])
        .with_columns([
            pl.int_range(pl.len()).add(1).alias('vaccination_seq').over(["id_x", "vaccine_name"])
        ])
        .with_columns(
            pl.when(
                (pl.col('vaccination_org') == 777777777777) &
                (pl.col('vaccine_name').is_in(['乙肝疫苗', '卡介苗'])) &
                (pl.col('vaccination_seq') == 1)
            ).then(pl.col('entry_org')).otherwise(pl.col('vaccination_org')).alias('vaccination_org')
        )
    )

    recommendations = calculate_all_vaccine_recommendations(person)