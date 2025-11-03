# 计算年龄和接种月龄，接种时间小于截止时间，剂次重排,五联疫苗及甲乙肝疫苗拆分
person=(
    pl.read_csv('/mnt/d/标准库接种率/data/person2.csv',schema_overrides={"vaccination_code":pl.String,"birth_date":pl.Datetime,"vaccination_date":pl.Datetime,"hepatitis_mothers":pl.String,"current_management_code":pl.String})
    .with_columns([
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias('expiration_date'),
        pl.lit(cutoff_date).str.to_date().dt.month_start().alias('mon_start'),
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias('mon_end')
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
        ).alias('age')])
    .with_columns([
    (
        (pl.col("vaccination_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
        (pl.col("vaccination_date").dt.month() - pl.col("birth_date").dt.month()) + pl.when(pl.col("vaccination_date").dt.day() >= pl.col("birth_date").dt.day())
        .then(0)
        .otherwise(-1)
    ).alias('vacc_month')])
    .with_columns([(
        (pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
        (pl.col("expiration_date").dt.month() - pl.col("birth_date").dt.month()) + pl.when(pl.col("expiration_date").dt.day() >= pl.col("birth_date").dt.day())
        .then(0)
        .otherwise(-1)
    ).alias('age_month')])
    .join(vaccine_tbl,left_on="vaccination_code",right_on="小类编码",how='left')
    .with_columns(pl.col("vaccine_name").str.split(","))
    .explode("vaccine_name")
    .sort(["id_x", "vaccine_name", "vaccination_date"])
    .with_columns([
        pl.int_range(pl.len()).add(1).alias('vaccination_seq').over(["id_x", "vaccine_name"])
    ])
    .with_columns(
        pl.when((pl.col('vaccination_org') == 777777777777) & (pl.col('vaccine_name').is_in(['乙肝疫苗','卡介苗'])) & (pl.col('vaccination_seq')==1))
        .then(pl.col('entry_org'))
        .otherwise(pl.col('vaccination_org'))
        .alias('vaccination_org')
    )
)


# 为每个人计算卡介苗的推荐时间
recommendations_BCG_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("0d").alias("recommended_dates"),
        pl.lit('卡介苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '卡介苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['卡介苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)


recommendations_HBV_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("0d").alias("recommended_dates"),
        pl.lit('乙肝疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '乙肝疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['乙肝疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_HBV_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '乙肝疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("1mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('乙肝疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '乙肝疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '乙肝疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
) 


recommendations_HBV_3 = (
    person
    .with_columns([
        # 使用over函数检查每个人是否存在第2剂乙肝疫苗记录
        ((pl.col("vaccination_seq") == 2) & 
         (pl.col("vaccine_name") == '乙肝疫苗'))
        .any()
        .over("id_x")
        .alias("has_dose2")
    ])
    .with_columns([
        # 基于第1剂的推荐时间计算（必须有第2剂记录才计算）
        pl.when(
            (pl.col("vaccination_seq") == 1) & 
            (pl.col("vaccine_name") == '乙肝疫苗') & 
            (pl.col("age") < 1) &
            (pl.col("has_dose2"))  # 必须有第2剂记录
        )
        .then(pl.col("vaccination_date").dt.offset_by("6mo"))  # <12月龄，与第1剂间隔6个月
        .when(
            (pl.col("vaccination_seq") == 1) & 
            (pl.col("vaccine_name") == '乙肝疫苗') & 
            (pl.col("age") >= 1) &
            (pl.col("has_dose2"))  # 必须有第2剂记录
        )
        .then(pl.col("vaccination_date").dt.offset_by("4mo"))  # ≥12月龄，与第1剂间隔4个月
        .otherwise(None)
        .alias("from_dose1"),
        
        # 再计算基于第2剂的推荐时间
        pl.when(
            (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") < 1)
        )
        .then(pl.col("vaccination_date").dt.offset_by("1mo"))   # <12月龄，与第2剂间隔1个月
        .when(
            (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") >= 1)
        )
        .then(pl.col("vaccination_date").dt.offset_by("60d"))   # ≥12月龄，与第2剂间隔60天
        .otherwise(None)
        .alias("from_dose2")
    ])
    .with_columns(
        pl.max_horizontal([
            pl.col("from_dose1").shift(),
            pl.col("from_dose2")
        ]).alias("recommended_dates"),
        pl.lit('乙肝疫苗').alias("recommended_vacc"),
        pl.lit(3).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '乙肝疫苗') &
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '乙肝疫苗') &
            (pl.col('vaccination_seq') == 2))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_HBV_4 = (
    person.with_columns([
        pl.when(
            (
                (pl.col("hepatitis_mothers") == '1') |  # 母亲HBsAg阳性（不管体重）
                ((pl.col("hepatitis_mothers") == '3') & (pl.col("birth_weight") < 2000))  # 母亲HBsAg不详且体重小于2000g
            ) & 
            (pl.col("vaccination_seq") == 3) & 
            (pl.col("vaccine_name") == '乙肝疫苗')
        )
        .then(pl.col("vaccination_date").dt.offset_by("5mo"))
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('乙肝疫苗').alias("recommended_vacc"),
        pl.lit(4).alias('recommended_seq')
    ])
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)


recommendations_POL_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("2mo").alias("recommended_dates"),
        pl.lit('脊灰疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['脊灰疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)


recommendations_POL_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '脊灰疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("3mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('脊灰疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_POL_3 = (
    person
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '脊灰疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("4mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('脊灰疫苗').alias("recommended_vacc"),
    pl.lit(3).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 2))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_POL_4 = (
    person
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 3) & (pl.col("vaccine_name") == '脊灰疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("4y"),
                pl.col("vaccination_date").dt.offset_by("1mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('脊灰疫苗').alias("recommended_vacc"),
    pl.lit(4).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 4) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 4) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 4) & (pl.col("大类名称") == '脊灰疫苗') &
            (pl.col('vaccination_seq') == 3))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DPT_1 = (
    person.with_columns([
        pl.col("birth_date").dt.offset_by("2mo").alias("recommended_dates"),
        pl.lit('百白破疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['百白破疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DPT_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '百白破疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("4mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('百白破疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DPT_3 = (
    person
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '百白破疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("6mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('百白破疫苗').alias("recommended_vacc"),
    pl.lit(3).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 3) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 2))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DPT_4 = (
    person
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 3) & (pl.col("vaccine_name") == '百白破疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("18mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('百白破疫苗').alias("recommended_vacc"),
    pl.lit(4).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 4) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 4) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 4) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 3))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DPT_5 = (
    person
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 4) & (pl.col("vaccine_name") == '百白破疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("6y"),
                pl.col("vaccination_date").dt.offset_by("1mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('百白破疫苗').alias("recommended_vacc"),
    pl.lit(5).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 5) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 5) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 5) & (pl.col("大类名称") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 4))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_DT_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("7y").alias("recommended_dates"),
        pl.lit('白破疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '白破疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['白破疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MCV_1 = (
    person.with_columns([
        pl.col("birth_date").dt.offset_by("8mo").alias("recommended_dates"),
        pl.lit('含麻疹成分疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '含麻疹成分疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['含麻疹成分疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MCV_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '含麻疹成分疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("18mo"),
                pl.col("vaccination_date").dt.offset_by("1mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('含麻疹成分疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '含麻疹成分疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '含麻疹成分疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MAV_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("6mo").alias("recommended_dates"),
        pl.lit('A群流脑疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("vaccine_name") == 'A群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("vaccine_name").is_in(['A群流脑疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MAV_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("9mo"),
                pl.col("vaccination_date").dt.offset_by("3mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('A群C群流脑疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == 'A群流脑疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == 'A群流脑疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) & (pl.col('vacc_month')<6)) 
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MAC_1 = (
    person
    .with_columns([
        # 为每个人标记A群流脑疫苗免疫剂次数
        ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
    ])
    .filter(
        # 合并三个条件：his_ma==2且vaccine_name=='A群流脑疫苗'，或his_ma==1且vaccine_name=='A群流脑疫苗'，或his_ma==0
        ((pl.col('his_ma') == 2) & (pl.col("vaccine_name") == 'A群流脑疫苗')) |
        ((pl.col('his_ma') == 1) & (pl.col("vaccine_name") == 'A群流脑疫苗')) |
        (pl.col('his_ma') == 0)
    )
    .with_columns([
        # 根据his_ma值设置不同的recommended_dates
        pl.when(pl.col('his_ma') == 2)
        .then(pl.col("birth_date").dt.offset_by("3y"))
        .when(pl.col('his_ma') == 1)
        .then(pl.max_horizontal([
            pl.col("birth_date").dt.offset_by("2y"),
            pl.col("vaccination_date").dt.offset_by("3mo")
        ]))
        .otherwise(pl.col("birth_date").dt.offset_by("2y"))
        .alias("recommended_dates"),
        
        pl.lit('A群C群流脑疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['A群C群流脑疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_MAC_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("6y"),
                pl.col("vaccination_date").dt.offset_by("3y")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('A群C群流脑疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)


recommendations_JE_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("8mo").alias("recommended_dates"),
        pl.lit('乙脑疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '乙脑疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['乙脑疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_JE_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '乙脑疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("2y"),
                pl.col("vaccination_date").dt.offset_by("12mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('乙脑疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '乙脑疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '乙脑疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_HAV_1 = (
    person
    .with_columns([
        pl.col("birth_date").dt.offset_by("18mo").alias("recommended_dates"),
        pl.lit('甲肝疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("大类名称") == '甲肝疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("大类名称").is_in(['甲肝疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)

recommendations_HAVL_2 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("小类名称") == '甲型肝炎灭活疫苗（人二倍体细胞）')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("2y"),
                pl.col("vaccination_date").dt.offset_by("6mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('甲肝疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '甲肝疫苗') &
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 2) & (pl.col("大类名称") == '甲肝疫苗') &
            (pl.col('vaccination_seq') == 1))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .group_by("id_x")
    .agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
)
