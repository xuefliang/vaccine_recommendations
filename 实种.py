import polars as pl


##卡介苗
# 实种当月,按接种单位
BCG_actual_1=(
    person
    .filter((pl.col('age')<4) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='卡介苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
BCG_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '卡介苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age') < 4) & 
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '卡介苗')
    )
    .filter(
        (pl.col('recommended_dates') <= pl.col('mon_end')) & 
        (pl.col('recommended_dates') >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

BCG_coverage_1=(
    BCG_actual_1.join(BCG_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##乙肝疫苗
# 实种当月,按接种单位
HBV_actual_1=(
    person
    .filter((pl.col('age')<18) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age') <= 6) & 
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates') <= pl.col('mon_end')) & 
        (pl.col('recommended_dates') >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_1=(
    HBV_actual_1.join(HBV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

# 实种当月,按接种单位
HBV_actual_2=(
    person
    .filter((pl.col('age')<18) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age') <= 6) & 
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates') <= pl.col('mon_end')) & 
        (pl.col('recommended_dates') >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_2=(
    HBV_actual_2.join(HBV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)


##############################################

HBV_expected_2=(
    recommendations
    .filter(pl.col.current_management_code==334878619388)
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age') <= 6) & 
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates') <= pl.col('mon_end')) & 
        (pl.col('recommended_dates') >= pl.col('mon_start').dt.offset_by("-1y"))
    )
)

tmp1=(pl.read_excel('/mnt/c/Users/Administrator/Downloads/工作簿1.xlsx'))

tmp2=(
    HBV_expected_2
    .filter(~pl.col.id_x.is_in(tmp1["A"].implode()))
)

tmp3=(
    person
    .filter(pl.col.id_x.is_in(tmp2['id_x'].implode()))
    .filter(pl.col.id_x=='14b9e5f586a24ef19e2292372e2cbd5f')
)

(
    BCG_coverage_1
    .filter(pl.col.vaccination_org==334878619388)
)


tmp5=(
    recommendations
    .filter(pl.col.id_x=='14b9e5f586a24ef19e2292372e2cbd5f')
    .filter(
        (pl.col('age') <= 6) & 
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
)


tmp6=(
    recommendations_HBV_2
    .filter(pl.col.id_x=='14b9e5f586a24ef19e2292372e2cbd5f')
    .filter(
        (pl.col('age') <= 6) & 
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
)