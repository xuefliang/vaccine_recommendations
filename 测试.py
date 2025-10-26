recommendations_MAV_2_p1 = (
    person
    .with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群流脑疫苗')
        )
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("9mo"),
                pl.col("vaccination_date").dt.offset_by("3mo")
            ])
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit('A群流脑疫苗').alias("recommended_vacc"),
        pl.lit(2).alias('recommended_seq')
    )
    .with_columns(
        # 判断是否显示推荐
        show_recommendation=(
            (pl.col("vaccine_name") == 'A群流脑疫苗') &
            (
                (pl.col('vaccination_seq') == 1) |  # 未种第二针
                (  # 或已种但延迟
                    (pl.col('vaccination_seq') == 2) &
                    (pl.col('vaccination_date') > pl.col('recommended_dates'))
                )
            )
        )
    )
    .filter(pl.col('show_recommendation'))
    .drop('show_recommendation')
)

recommendations_MAV_2_p2 = (
    person
    .with_columns(
        # 推荐日期
        recommended_dates=pl.when(
            (pl.col("vaccination_seq") == 1) & 
            (pl.col("vaccine_name") == 'A群C群流脑疫苗') & 
            (pl.col('vacc_month') < 24)
        ).then(pl.col("vaccination_date").dt.offset_by("3mo")),
        
        recommended_vacc=pl.lit('A群流脑疫苗'),
        recommended_seq=pl.lit(2),
    )
    .with_columns(
        # 判断是否需要显示（第二针未种或延迟接种）
        show_recommendation=(
            (pl.col("vaccine_name") == 'A群C群流脑疫苗') &
            (
                (pl.col('vaccination_seq') == 1) |  # 未种第二针
                (  # 或已种但延迟
                    (pl.col('vaccination_seq') == pl.when(pl.col('vacc_month') < 6).then(3).otherwise(2)) &
                    (pl.col('vaccination_date') > pl.col('recommended_dates'))
                )
            )
        )
    )
    .filter(pl.col('show_recommendation'))  # 只保留需要推荐的记录
    .drop('show_recommendation')
)


recommendations=(
    pl.concat([recommendations_MAV_2_p1,recommendations_MAV_2_p2])
    .group_by("id_x").agg(
        [
            pl.col("recommended_dates").drop_nulls().first().alias("recommended_dates"),
            pl.col("recommended_vacc").first().alias("recommended_vacc"),
            pl.col("recommended_seq").first().alias("recommended_seq"),
            pl.col("birth_date").first().alias("birth_date"),
            pl.col("age").first().alias("age"),
            pl.col("age_month").first().alias("age_month"),
            pl.col("entry_org").first().alias("entry_org"),
            pl.col("entry_date").first().alias("entry_date"),
            pl.col("vaccination_org").first().alias("vaccination_org"),
            pl.col("current_management_code").first().alias("current_management_code"),
        ]
    )
)


recommendations_MAV_1_p1 = (
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
)

recommendations_MAV_1_p1 = (
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
)

recommendations_MAV_1_p2 = (
    recommendations_MAV_1_p1
    .filter((pl.col('recommended_seq') == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) & 
            (pl.col('vacc_month') <24))
)

recommendations_MAV_1=(
    recommendations_MAV_1_p1
    .filter(~pl.col('id_x').is_in(recommendations_MAV_1_p2['id_x'].implode()))
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
            (pl.col('recommended_seq') == 1) & 
            (pl.col("vaccine_name") == 'A群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~pl.col("vaccine_name").is_in(['A群流脑疫苗']))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .filter(
        ~(
            (pl.col('recommended_seq') == 1) & 
            (pl.col("vaccine_name") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) & 
            (pl.col('vacc_month') < 24)
        )
    )
)

tmp=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/新建 Microsoft Excel 工作表.xlsx')
)

tmp2=(
    recommendations
    .filter(pl.col.id_x.is_in(tmp['A'].implode()))
    .filter(pl.col.recommended_vacc=='A群流脑疫苗')
    .filter(pl.col.recommended_seq==2)
)

tmp3=(
    tmp
    .filter(~pl.col.A.is_in(tmp2["id_x"].implode()))
)

tmp4=(
    person
    .filter(pl.col.id_x.is_in(tmp3["A"].implode()))
)

tmp5=(
    recommendations
    .filter(pl.col.id_x=='e652b8b3c3a64d99b47c59332a82b183')
)

