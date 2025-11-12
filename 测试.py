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

temp=(
    person_vacc
    .filter(pl.col.person_id=='7bbf0019e1464f6c9d196de612398f27')
)

temp2=(
    recommendations
    .filter(pl.col.person_id=='7bbf0019e1464f6c9d196de612398f27')
)

actual=(
    calculate_actual_vaccination(person_vacc, "乙肝疫苗", 1, 18*12)
    .filter(pl.col.vaccination_org=='307473238584')
)

tmp1=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/新建 Microsoft Excel 工作表.xlsx')
)

tmp2=(
    recommendations
    .filter(pl.col.person_id.is_in(tmp1['A'].implode()))
    .filter(pl.col.recommended_vacc=='百白破疫苗')
    .filter(pl.col.recommended_seq==5)
    .select(['person_id','recommended_dates'])
)

tmp3=(
    tmp1.join(tmp2,left_on='A',right_on='person_id',how='left')
    .with_columns((pl.col.B-pl.col.recommended_dates.dt.date()).alias('差异'))
)

tmp4=(
    recommendations
    .filter(pl.col.recommended_vacc=='百白破疫苗')
    .filter(pl.col.recommended_seq==5)
    .filter(pl.col.current_management_code=='307473238584')
    .filter(pl.col("age_month") <= 7*12)
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
)

tmp5=(
    tmp4
    .filter(~pl.col.person_id.is_in(tmp1['A'].implode()))
)

tmp6=(
    person_vacc
    .filter(pl.col.person_id=='00771c80b8e94843b28e90d94735103c')
    .filter(pl.col.vaccine_name.is_in(['百白破疫苗','白破疫苗']))
)


recommendations_DPT_5 = (
    person_vacc
    .with_columns(
            ((pl.col("vaccine_name") == "百白破疫苗"))
            .sum()
            .over("person_id")
            .alias("his_dpt"),
            ((pl.col("vaccine_name") == "白破疫苗"))
            .sum()
            .over("person_id")
            .alias("his_dt"))
    .filter(~((pl.col('his_dpt')==4) & (pl.col('his_dt')==1)))
    .with_columns(
        pl.when(
        (pl.col("vaccination_seq") == 4) & (pl.col("vaccine_name") == '百白破疫苗')
    )
    .then(
        pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("6y"),
                pl.col("vaccination_date").dt.offset_by("12mo")
        ])
    )
    .otherwise(None)
    .alias("recommended_dates"),
    pl.lit('百白破疫苗').alias("recommended_vacc"),
    pl.lit(5).alias('recommended_seq')
    )
    .with_columns(
        pl.when(
            (pl.col('recommended_seq') == 5) & (pl.col("vaccine_name") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 5) &
            (pl.col('vaccination_date') > pl.col('recommended_dates'))
        )
        .then(pl.col('recommended_dates'))
        .when(
            (pl.col('recommended_seq') == 5) & (pl.col("vaccine_name") == '百白破疫苗') &
            (pl.col('vaccination_seq') == 4))
        .then(pl.col('recommended_dates'))
        .otherwise(None)
    )
)

(
    recommendations
    .filter(pl.col.recommended_vacc=='HPV疫苗')
)

(
    period_coverage
    .filter(pl.col('疫苗名称')=='HPV疫苗')
)

tmp6=(
    person_vacc
    .filter(pl.col('vaccine_name')=='HPV疫苗')
    .filter(
        (pl.col('vaccination_date').dt.date() <= pl.col('mon_end')) & 
        (pl.col('vaccination_date').dt.date() >= pl.col('mon_start'))
    )
)

temp=(
    recommendations
    .filter(pl.col.recommended_vacc=='HPV疫苗')
    .filter(pl.col.gender_code==2)
)

temp=(
    person_vacc
    .filter(
                (pl.col("gender_code") == 2)  # 仅女性
                & (pl.col("age_month") <= 14*12)  # 年龄限制
                & (pl.col("vaccine_name") == "HPV疫苗")  # 疫苗名称
                & (pl.col("age_month") >= 13*12)
                & (pl.col("vaccination_date").dt.date() >= pl.col("mon_start"))  # 当月开始
                & (pl.col("vaccination_date").dt.date() <= pl.col("mon_end")) 
    )
)

tmp1=(
    person_vacc
    .filter(
        (pl.col.person_id.is_in(['0b1d48d65bf04ff286ca36b8f1bd5f35']))
        & (pl.col("vaccine_name") == "HPV疫苗") 
    )
)

        hpv_records=(person_vacc.filter(
            (pl.col("gender_code") == 2)
            & (pl.col("vaccine_name") == "HPV疫苗")
            & (pl.col("birth_date") >= pl.date(2011,11,11))
            & (pl.col("age_month") < 14*12)
        ).select([
            "person_id",
            "birth_date",
            "vaccination_seq",
            "vaccination_date",
            "vaccination_org",
            "vaccine_name",
            "mon_start",
            "mon_end"
        ]))
        
        # 获取第1剂接种记录
        dose1 = hpv_records.filter(pl.col("vaccination_seq") == 1).select([
            pl.col("person_id"),
            pl.col("vaccination_date").alias("dose1_date")
        ])
        
        # 获取第2剂接种记录（在统计期内）
        dose2 = hpv_records.filter(
            (pl.col("vaccination_seq") == 2)
            & (pl.col("vaccination_date").dt.date() >= pl.col("mon_start"))
            & (pl.col("vaccination_date").dt.date() <= pl.col("mon_end"))
        ).select([
            pl.col("person_id"),
            pl.col("vaccination_date").alias("dose2_date"),
            pl.col("vaccination_org"),
            pl.col("vaccine_name"),
            pl.col("vaccination_seq")
        ])
        
        # 获取第3剂接种记录（在统计期内）
        dose3 = hpv_records.filter(
            (pl.col("vaccination_seq") == 3)
            & (pl.col("vaccination_date").dt.date() >= pl.col("mon_start"))
            & (pl.col("vaccination_date").dt.date() <= pl.col("mon_end"))
        ).select([
            pl.col("person_id"),
            pl.col("vaccination_date").alias("dose3_date"),
            pl.col("vaccination_org"),
            pl.col("vaccine_name")
        ])
        
        # 情况1：第1剂和第2剂间隔≥5个月，统计第2剂
        valid_dose2 = (
            dose1.join(dose2, on="person_id", how="inner")
            .with_columns([
                # 计算间隔天数
                (pl.col("dose2_date") - pl.col("dose1_date")).dt.total_days().alias("interval_days")
            ])
            .filter(pl.col("interval_days") >= 150)  # 5个月 ≈ 150天
            .with_columns([
                pl.lit(2).cast(pl.Int64).alias("vaccination_seq")
            ])
            .select(['person_id','vaccination_org','vaccine_name','vaccination_seq'])
        )
        
        # 情况2：第1剂和第2剂间隔<5个月，统计第3剂
        # 先找出间隔<5个月的人员ID
        short_interval_ids = (
            dose1.join(
                hpv_records.filter(pl.col("vaccination_seq") == 2).select([
                    pl.col("person_id"),
                    pl.col("vaccination_date").alias("dose2_date")
                ]), 
                on="person_id", 
                how="inner"
            )
            .with_columns([
                (pl.col("dose2_date") - pl.col("dose1_date")).dt.total_days().alias("interval_days")
            ])
            .filter(pl.col("interval_days") < 150)  # 间隔<5个月
            .select("person_id")
        )
        
        # 统计这些人的第3剂（在统计期内）
        valid_dose3 = (
            short_interval_ids.join(dose3, on="person_id", how="inner")
            .with_columns([
                pl.lit(2).cast(pl.Int64).alias("vaccination_seq")  # 统计为第2剂
            ])
            .select(['person_id','vaccination_org','vaccine_name','vaccination_seq'])
        )
        
        
        # 合并两种情况
        parts_to_concat = []
        if valid_dose2.height > 0:
            parts_to_concat.append(valid_dose2)
        if valid_dose3.height > 0:
            parts_to_concat.append(valid_dose3)
        
        if parts_to_concat:
            return (
                pl.concat(parts_to_concat)
                .group_by(["vaccination_org", "vaccine_name", "vaccination_seq"])
                .agg(pl.col("person_id").len().alias("vac"))
            )
        else:
            # 返回空DataFrame但保持schema一致
            return pl.DataFrame(
                schema={
                    "vaccination_org": pl.Int64,
                    "vaccine_name": pl.Utf8,
                    "vaccination_seq": pl.Int64,
                    "vac": pl.Int64,
                }
            )
    
    # 其他剂次：正常统计
    else:
        return (
            person.filter(
                (pl.col("gender_code") == 2)  # 仅女性
                & (pl.col("age_month") < actual_max_age)  # 年龄限制
                & (pl.col("vaccination_seq") == seq)  # 接种序号
                & (pl.col("vaccine_name") == "HPV疫苗")  # 疫苗名称
                & (pl.col("vaccination_date").dt.date() >= pl.col("mon_start"))  # 当月开始
                & (pl.col("vaccination_date").dt.date() <= pl.col("mon_end"))  # 当月结束
            )
            .group_by(["vaccination_org", "vaccine_name", "vaccination_seq"])
            .agg(pl.col("person_id").n_unique().alias("vac"))  # 统计唯一人数
        )