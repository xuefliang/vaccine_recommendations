recommendations_HBV_2 = (
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
    .filter(pl.col.id_x.is_in(['0169c6794eb840c7a6d9a8b20ded00ab','0966e6b5900d4d24abfc7c3646c44b9b','0369b86e43ee46109b6afa2ba406ea32','0b9401273f34429bbff2c8b833cc9faf','475370d079604680980250a38faecbf8']))
    .filter(pl.col.recommended_vacc=='乙肝疫苗')
    .filter(pl.col.recommended_seq==3)
    .filter(pl.col.大类名称=='乙肝疫苗')
    .select(['id_x','age_month','vaccination_date','vaccination_seq','大类名称','recommended_dates'])
) 

(
    recommendations_HBV_2
    .to_pandas()
    .to_excel('/mnt/c/Users/Administrator/Downloads/cd.xlsx')
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
)


tmp=(
        recommendations
        .filter(pl.col.recommended_vacc=='A群流脑疫苗')
        .filter(pl.col.recommended_seq==2)
    )
