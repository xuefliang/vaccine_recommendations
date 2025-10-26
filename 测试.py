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

MAV_actual_2_p1=(
    person
    .filter((pl.col('age_month')<2*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='A群流脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

MAV_actual_2_p2 = (
    person
    .filter((pl.col('age_month') < 2*12))
    .filter(
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
        (pl.col('vaccination_seq') == 1) & 
        (pl.col('vacc_month') < 6)
    )
    .select('id_x')
    .unique()
    .join(
        person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date') >= pl.col('mon_start')) & 
            (pl.col('vaccination_date') <= pl.col('mon_end'))
        ),
        on='id_x',
        how='inner'
    )
    .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

MAV_actual_2_p3=(
    person
    .filter((pl.col('age_month') < 2*12))
    .filter(
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
        (pl.col('vaccination_seq') == 1) & 
        (pl.col('vacc_month') >= 6)
    )
    .select('id_x')
    .unique()
    .join(
        person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date') >= pl.col('mon_start')) & 
            (pl.col('vaccination_date') <= pl.col('mon_end'))
        ),
        on='id_x',
        how='inner'
    )
    .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

# 预先筛选基础数据
base_person = person.filter(pl.col('age_month') < 2*12)

# 符合条件的id集合
eligible_ids_p2 = (
    base_person
    .filter(
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
        (pl.col('vaccination_seq') == 1) & 
        (pl.col('vacc_month') < 6)
    )
    .select('id_x')
    .unique()
)

eligible_ids_p3 = (
    base_person
    .filter(
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
        (pl.col('vaccination_seq') == 1) & 
        (pl.col('vacc_month') >= 6)
    )
    .select('id_x')
    .unique()
)

# 合并结果
MAV_actual_2 = pl.concat([
    # p1: A群流脑疫苗 第2剂
    base_person
    .filter(
        (pl.col('vaccination_seq') == 2) &
        (pl.col('vaccine_name') == 'A群流脑疫苗') &
        (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
    )
    .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac')),
    
    # p2: A群C群流脑疫苗 第3剂 -> 统计为 A群流脑疫苗
    eligible_ids_p2.join(
        base_person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
        ),
        on='id_x',
        how='inner'
    )
    .with_columns(pl.lit('A群流脑疫苗').alias('vaccine_name'))
    .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac')),
    
    # p3: A群C群流脑疫苗 第2剂 -> 统计为 A群流脑疫苗
    eligible_ids_p3.join(
        base_person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
        ),
        on='id_x',
        how='inner'
    )
    .with_columns(pl.lit('A群流脑疫苗').alias('vaccine_name'))
    .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
]).group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq']).agg(
    pl.col('vac').sum().alias('vac')
)



#应种当月-1y，按管理单位
MAV_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == 'A群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & (pl.col('vacc_month')<24) &
                (
                    (pl.col('vaccine_name') == 'A群流脑疫苗') |
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗')
                ) &
                (
                    (pl.col("vaccination_date") <= pl.col('mon_end'))
                )
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == 'A群流脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
)



tmp=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/新建 Microsoft Excel 工作表.xlsx')
)

tmp1=(
    MAV_expected_2
    .filter(pl.col.current_management_code==392423210604)
)

tmp2=(
    tmp1
    .filter(~pl.col.id_x.is_in(tmp['A'].implode()))
)

tmp3=(
    recommendations
    .filter(pl.col.id_x.is_in(tmp2['A'].implode()))
    .filter(pl.col.recommended_vacc=='A群流脑疫苗')
)

tmp4=(
    MAV_expected_2
    .filter(pl.col.id_x=='db5f286fd316458b9cd091f22f46489b')
)

tmp5=(
    person
    .filter(pl.col.id_x=='db5f286fd316458b9cd091f22f46489b')
)


MAV_expected_2 = (
    recommendations
    # 排除条件1：已接种第2剂A群流脑疫苗的人群
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == 'A群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    # 排除条件2：第1剂接种A群C群流脑疫苗时vacc_month<6，且已接种3剂次A群C群流脑疫苗的人群
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) &
                (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                (pl.col('vacc_month') < 6)
            )
            .select('id_x')
            .join(
                person.filter(
                    (pl.col('vaccination_seq') == 3) &
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                    (pl.col("vaccination_date") <= pl.col('mon_end'))
                ).select('id_x'),
                on='id_x',
                how='inner'
            )
            ['id_x'].implode()
        )
    )
    # 排除条件3：第1剂接种A群C群流脑疫苗时vacc_month>=6，且已接种2剂次A群C群流脑疫苗的人群
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) &
                (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                (pl.col('vacc_month') >= 6)
            )
            .select('id_x')
            .join(
                person.filter(
                    (pl.col('vaccination_seq') == 2) &
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                    (pl.col("vaccination_date") <= pl.col('mon_end'))
                ).select('id_x'),
                on='id_x',
                how='inner'
            )
            ['id_x'].implode()
        )
    )
    # 筛选条件：接种过第1剂且vacc_month<24的人群
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vacc_month') < 24) &
                (
                    (pl.col('vaccine_name') == 'A群流脑疫苗') |
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗')
                ) &
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    # 筛选推荐序列和疫苗类型
    .filter(
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == 'A群流脑疫苗')
    )
    # 筛选推荐日期范围
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
)