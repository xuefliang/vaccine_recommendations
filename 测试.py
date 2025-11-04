import polars as pl

recommendations_MAC_1 = (
    person
    .with_columns([
        # 为每个人标记A群流脑疫苗免疫剂次数
        ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
        # A群C群流脑疫苗 - 24月龄及以后的接种次数
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col("vacc_month") >=24) & (pl.col('vaccination_date')<=pl.col('mon_end'))).sum().over("id_x").alias("his_mac"),
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col("vacc_month") <24) & (pl.col('vaccination_date')<=pl.col('mon_end'))).sum().over("id_x").alias("his_mac_before")
    ])
    .filter(
        # 合并三个条件
        ((pl.col('his_ma') == 2) & (pl.col("vaccine_name").is_in(['A群流脑疫苗','A群C群流脑疫苗']))) |
        ((pl.col('his_ma') == 1) & (pl.col("vaccine_name").is_in(['A群流脑疫苗','A群C群流脑疫苗']))) |
        (pl.col('his_ma') == 0)
    )
    .with_columns([
        # 根据his_ma值设置不同的recommended_dates
        pl.when((pl.col('his_ma') == 2) & (pl.col('his_mac') == 0))
        .then(pl.col("birth_date").dt.offset_by("3y"))
        .when((pl.col('his_ma') < 2) & (pl.col('his_mac') == 1))
        .then((pl.col("birth_date").dt.offset_by("3y")))
        .when((pl.col('his_ma') == 0) & (pl.col('his_mac')==0) & (pl.col('his_mac_before')==0))
        .then((pl.col("birth_date").dt.offset_by("2y")))
        .when((pl.col('his_ma') == 0) & (pl.col('his_mac')==0) & (pl.col('his_mac_before')>=2))
        .then((pl.col("birth_date").dt.offset_by("3y")))
        .when((pl.col('his_ma') == 0) & (pl.col('his_mac')==0) & (pl.col('his_mac_before')==1))
        .then((pl.col("birth_date").dt.offset_by("2y")))
        .when((pl.col('his_ma') == 1) & (pl.col('his_mac')==0) & (pl.col('his_mac_before')==0) & (pl.col('vaccine_name')=='A群流脑疫苗'))
        .then(pl.max_horizontal([
            pl.col("birth_date").dt.offset_by("2y"),
            pl.col("vaccination_date").dt.offset_by("3mo")]))
        .alias("recommended_dates"),
        
        pl.lit('A群C群流脑疫苗').alias("recommended_vacc"),
        pl.lit(1).alias('recommended_seq')
    ])
    .with_columns(
        pl.when((pl.col('his_mac')==1))
        .then(None)
        .otherwise(pl.col("recommended_dates"))
        .alias("recommended_dates")
    )
    .with_columns(
        pl.when((pl.col('his_ma') == 2) & (pl.col('his_mac') == 0) & (pl.col('his_mac_before') == 0) & (pl.col('vaccine_name')=='A群C群流脑疫苗') & (pl.col('vacc_month')>=24))
        .then(None)
        .otherwise(pl.col("recommended_dates"))
        .alias("recommended_dates")
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗') &
            (pl.col('vaccination_seq') == 1) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(~(pl.col("vaccine_name").is_in(['A群C群流脑疫苗'])))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
    .filter(pl.col.current_management_code==392423210604)
    .filter(pl.col.recommended_vacc=='A群C群流脑疫苗')
    .filter(pl.col.recommended_seq==1)
    .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y")))

)

recommendations_MAC_2 = (
    person
    .with_columns([
        # 为每个人标记A群流脑疫苗免疫剂次数
        ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
        # A群C群流脑疫苗 - 24月龄及以后的接种次数
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col("vacc_month") >=24) & (pl.col("vacc_month") <60) & (pl.col('vaccination_date')<=pl.col('mon_end'))).sum().over("id_x").alias("his_mac"),
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col("vacc_month") >=60) & (pl.col('vaccination_date')<=pl.col('mon_end'))).sum().over("id_x").alias("his_mac_5"),
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col('vaccination_date')<=pl.col('mon_end'))).sum().over("id_x").alias("ac_max_seq")
    ])
    .with_columns(
        pl.when((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col('his_mac')==1)  & (pl.col('his_mac_5')==0))
        .then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("6y"),
                pl.col("vaccination_date").dt.offset_by("3y")
            ])
        )
        .when((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗') & (pl.col('his_mac_5')==1) & (pl.col('his_mac')==0) & (pl.col('vaccination_seq')==pl.col('ac_max_seq')))
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
    .filter(pl.col.current_management_code==392423210604)
    .filter(pl.col.recommended_vacc=='A群C群流脑疫苗')
    .filter(pl.col.recommended_seq==2)
    .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y")))
)

tmp6=(
    person
    .filter(pl.col.id_x=='02c9b8c9efb44c48b04961665f7d5a53')
    .with_columns([
        # 为每个人标记A群流脑疫苗免疫剂次数
        ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
        # A群C群流脑疫苗 - 24月龄前的接种次数
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & 
         (pl.col("vacc_month") < 24) & 
         (pl.col('vaccination_date') <= pl.col('mon_end'))).sum().over("id_x").alias("his_mac_before24"),
        # A群C群流脑疫苗 - 24月龄及以后的接种次数
        ((pl.col("vaccine_name") == 'A群C群流脑疫苗') & 
         (pl.col("vacc_month") >= 24) & 
         (pl.col('vaccination_date') <= pl.col('mon_end'))).sum().over("id_x").alias("his_mac_after24")
    ])
)


temp=(
    recommendations
    .filter(pl.col.current_management_code==392423210604)
    .filter(pl.col.recommended_vacc=='A群C群流脑疫苗')
    .filter(pl.col.recommended_seq==2)
    .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y")))
    .filter(pl.col.id_x.is_unique())
)

tmp1=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/新建 Microsoft Excel 工作表.xlsx')
)

tmp2=(
    tmp1.filter(~pl.col.A.is_in(temp['id_x'].implode()))
)

tmp3=(
    person
    .filter(pl.col.id_x=='660d468897da4961b018298a8334fa80')
    .filter(pl.col("vaccine_name").is_in(['A群流脑疫苗','A群C群流脑疫苗']))
)

tmp4=(
    temp
    .filter(~pl.col.id_x.is_in(tmp1['A'].implode()))    
)

# AC1实种
AC1_actual=(
    person
    .with_columns(
        ((pl.col('age_month') < 18*12) & 
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
        (pl.col('vacc_month')>24)).sum().over('id_x').alias("his_mac")
    )
    .filter(
        (pl.col('his_mac')==1) & (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
        (pl.col('vaccination_date').dt.date() >= pl.col('mon_start')) & 
        (pl.col('vaccination_date').dt.date() <= pl.col('mon_end')))
    .filter(pl.col.vaccination_org==392423210604)
    .filter(pl.col.id_x.is_unique())
)

# AC1应种
AC1_expect=(
    recommendations
    .filter(pl.col.current_management_code==392423210604)
    .filter(pl.col.recommended_vacc=='A群C群流脑疫苗')
    .filter(pl.col.recommended_seq==1)
    .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y")))
    .filter(pl.col.id_x.is_unique())
)

# AC2实种
AC2_actual=(
    person
    .with_columns(
        ((pl.col('age_month') < 18*12) & 
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
        (pl.col('vacc_month')>=24)).sum().over('id_x').alias("his_mac"),
        ((pl.col('age_month') < 18*12) & 
        (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
        (pl.col('vacc_month')>60)).sum().over('id_x').alias("his_mac_60")
    )
    .filter(
        (pl.col('his_mac')==2) & (pl.col('his_mac_60')>=1) & (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
        (pl.col('vaccination_date').dt.date() >= pl.col('mon_start')) & 
        (pl.col('vaccination_date').dt.date() <= pl.col('mon_end')))
    .filter(pl.col.vaccination_org==392423210604)
    .filter(pl.col.id_x.is_unique())
)

# AC2应种
AC2_expect=(
    recommendations
    .filter(pl.col.current_management_code==392423210604)
    .filter(pl.col.recommended_vacc=='A群C群流脑疫苗')
    .filter(pl.col.recommended_seq==2)
    .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y")))
    .filter(pl.col.id_x.is_unique())
)


tmp2=(
    AC2_actual
    .filter(
        ~pl.col.id_x.is_in(tmp1['A'].implode())
    )
)

tmp3=(
    tmp1
    .filter(
        ~pl.col.A.is_in(AC1_actual['id_x'].implode())
    )
)


tmp4=(
    person
    .filter(pl.col.id_x.is_in(tmp2['id_x'].implode()))
    .filter(pl.col("vaccine_name").is_in(['A群流脑疫苗','A群C群流脑疫苗']))
)

tmp5=(
    person
    .filter(pl.col.id_x.is_in(tmp3['A'].implode()))
    .filter(pl.col("vaccine_name").is_in(['A群流脑疫苗','A群C群流脑疫苗']))
)

def lowercase(df: pl.DataFrame) -> pl.DataFrame:
    """将 DataFrame 所有列名转换为小写"""
    return df.rename({col: col.lower() for col in df.columns})

person=pl.read_csv('/mnt/c/Users/Administrator/Downloads/标准库接种率+v1.0.9-2024-12-27/标准库数据/person_standard.csv').pipe(lowercase)

vaccination=pl.read_csv('/mnt/c/Users/Administrator/Downloads/标准库接种率+v1.0.9-2024-12-27/标准库数据/person_standard_vaccination.csv').pipe(lowercase)

person=(
    vaccination.join(person,left_on='person_id',right_on='id',how='left')
)