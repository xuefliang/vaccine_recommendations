import polars as pl

# 实种当月,按接种单位
actual=(
    person
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter(pl.col('age')<18)
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
expected=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .group_by(['current_management_code', 'recommended_vacc','recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

coverage=(
    actual.join(expected,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)



##卡介苗
# 实种当月,按接种单位
BCG_actual_1=(
    person
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<4) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='卡介苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
BCG_expected_1=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<4) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('recommended_vacc')=='卡介苗')
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .group_by(['current_management_code', 'recommended_vacc','recommended_seq'])
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
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<18) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_1=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<6) & (pl.col('recommended_seq')==1))
    .filter(pl.col('recommended_vacc')=='乙肝疫苗')
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .group_by(['current_management_code', 'recommended_vacc','recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_1=(
    HBV_actual_1.join(HBV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##乙肝疫苗
# 实种当月,按接种单位
HBV_actual_2=(
    person
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<18) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_2=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age_month')>1) & (pl.col('age')<6) & (pl.col('recommended_seq')==2))
    .filter(pl.col('recommended_vacc')=='乙肝疫苗')
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .group_by(['current_management_code', 'recommended_vacc','recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_2=(
    HBV_actual_1.join(HBV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##乙肝疫苗
# 实种当月,按接种单位
HBV_actual_3=(
    person
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<18) & (pl.col('vaccination_seq')==3))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_3=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age_month')>6) & (pl.col('age')<6) & (pl.col('recommended_seq')==3))
    .filter(pl.col('recommended_vacc')=='乙肝疫苗')
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .group_by(['current_management_code', 'recommended_vacc','recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_3=(
    HBV_actual_3.join(HBV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='inner')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)


tmp=(
    coverage.filter(pl.col.vaccination_org==333647265032)
    .sort(['vaccine_name', 'vaccination_seq'])
)


tmp2=(
    person
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .filter(pl.col.vaccination_org==333647265032)
    .filter(pl.col.vaccine_name=='乙肝疫苗')
    .filter(pl.col.vaccination_seq==1)
)

tmp=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/tmp.xlsx')
)

tmp4=(
    tmp.filter(~pl.col.id.is_in(recommendations_HBV_1['id_x']))
)

HBV_expected_1=(
    recommendations
    .with_columns(pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
    pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end'))
    .filter((pl.col('age')<6) & (pl.col('recommended_seq')==1))
    .filter(pl.col('recommended_vacc')=='乙肝疫苗')
    .filter((pl.col('recommended_dates')<=pl.col('mon_end')) & (pl.col('recommended_dates')>=pl.col('mon_start').dt.offset_by("-1y")))
    .filter(pl.col.current_management_code==333647265032)
    # .filter(pl.col.id_x=='0363a462296043f6ae98a5212e0c601f')
)
