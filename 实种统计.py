import polars as pl

##卡介苗
# 实种当月,按接种单位
BCG_actual_1=(
    person
    .filter((pl.col('age_month')<4*12) & (pl.col('vaccination_seq')==1))
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
        (pl.col('age_month') < 4*12) &
        (pl.col('age_month') > 0)
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '卡介苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

BCG_coverage_1=(
    BCG_actual_1.join(BCG_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##乙肝疫苗
# 实种当月,按接种单位
HBV_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
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
        (pl.col('age_month') <= 6*12) &
        (pl.col('age_month') > 0) 
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_1=(
    HBV_actual_1.join(HBV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

# 实种当月,按接种单位
HBV_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
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
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month') > 1) & 
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_2=(
    HBV_actual_2.join(HBV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

# 实种当月,按接种单位
HBV_actual_3=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==3))
    .filter(pl.col('vaccine_name')=='乙肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
HBV_expected_3=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 3) & 
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
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '乙肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) &
        (pl.col('age_month') > 6) & 
        (pl.col('recommended_seq') == 3) & 
        (pl.col('recommended_vacc') == '乙肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HBV_coverage_3=(
    HBV_actual_3.join(HBV_expected_3,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##脊灰疫苗
# 实种当月,按接种单位
POL_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='脊灰疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
POL_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=2) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '脊灰疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

POL_coverage_1=(
    POL_actual_1.join(POL_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

POL_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='脊灰疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
POL_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
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
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=3) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '脊灰疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

POL_coverage_2=(
    POL_actual_2.join(POL_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

POL_actual_3=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==3))
    .filter(pl.col('vaccine_name')=='脊灰疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

POL_expected_3=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 3) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=4) &
        (pl.col('recommended_seq') == 3) & 
        (pl.col('recommended_vacc') == '脊灰疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

POL_coverage_3=(
    POL_actual_3.join(POL_expected_3,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

POL_actual_4=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==4))
    .filter(pl.col('vaccine_name')=='脊灰疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

POL_expected_4=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 4) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 3) & 
                (pl.col('vaccine_name') == '脊灰疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=4*12) &
        (pl.col('recommended_seq') == 4) & 
        (pl.col('recommended_vacc') == '脊灰疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

POL_coverage_4=(
    POL_actual_4.join(POL_expected_4,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)


##百白破疫苗
# 实种当月,按接种单位
DPT_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='百白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
DPT_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=2) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '百白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DPT_coverage_1=(
    DPT_actual_1.join(DPT_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

DPT_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='百白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
DPT_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
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
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=4) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '百白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DPT_coverage_2=(
    DPT_actual_2.join(DPT_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

DPT_actual_3=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==3))
    .filter(pl.col('vaccine_name')=='百白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

DPT_expected_3=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 3) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=6) &
        (pl.col('recommended_seq') == 3) & 
        (pl.col('recommended_vacc') == '百白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DPT_coverage_3=(
    DPT_actual_3.join(DPT_expected_3,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

DPT_actual_4=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==4))
    .filter(pl.col('vaccine_name')=='百白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

DPT_expected_4=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 4) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 3) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=18) &
        (pl.col('recommended_seq') == 4) & 
        (pl.col('recommended_vacc') == '百白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DPT_coverage_4=(
    DPT_actual_4.join(DPT_expected_4,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

DPT_actual_5=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==5))
    .filter(pl.col('vaccine_name')=='百白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

DPT_expected_5=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 5) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 4) & 
                (pl.col('vaccine_name') == '百白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') < 7*12) & 
        (pl.col('age_month')>=6*12) &
        (pl.col('recommended_seq') == 5) & 
        (pl.col('recommended_vacc') == '百白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DPT_coverage_5=(
    DPT_actual_5.join(DPT_expected_5,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

##白破
DT_actual_1=(
    person
    .filter((pl.col('age_month')<12*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='白破疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

DT_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '白破疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 11*12) & 
        (pl.col('age_month')>=6*12) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '白破疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

DT_coverage_1=(
    DT_actual_1.join(DT_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

###含麻疹成分疫苗
MCV_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='含麻疹成分疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

MCV_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '含麻疹成分疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=8) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '含麻疹成分疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MCV_coverage_1=(
    MCV_actual_1.join(MCV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

MCV_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='含麻疹成分疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
MCV_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '含麻疹成分疫苗') & 
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
                (pl.col('vaccine_name') == '含麻疹成分疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=18) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '含麻疹成分疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MCV_coverage_2=(
    MCV_actual_2.join(MCV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

### A群流脑疫苗
MAV_actual_1=(
    person
    .filter((pl.col('age_month')<2*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='A群流脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

MAV_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == 'A群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 23) & 
        (pl.col('age_month')>=6) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == 'A群流脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MAV_coverage_1=(
    MAV_actual_1.join(MAV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

MAV_actual_2=(
    person
    .filter((pl.col('age_month')<2*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='A群流脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
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
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == 'A群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month')<=23) & 
        (pl.col('age_month')>=9) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == 'A群流脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MAV_coverage_2=(
    MAV_actual_2.join(MAV_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

### AC群流脑疫苗
MAC_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='A群C群流脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

MAC_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=2*12) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == 'A群C群流脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MAC_coverage_1=(
    MAC_actual_1.join(MAC_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

MAC_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='A群C群流脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
MAC_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
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
                (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month')<=8*12) & 
        (pl.col('age_month')>=5*12) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == 'A群C群流脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

MAC_coverage_2=(
    MAC_actual_2.join(MAC_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

###乙脑
JE_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='乙脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

JE_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '乙脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=8) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '乙脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

JE_coverage_1=(
    JE_actual_1.join(JE_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)

JE_actual_2=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==2))
    .filter(pl.col('vaccine_name')=='乙脑疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

#应种当月-1y，按管理单位
JE_expected_2=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 2) & 
                (pl.col('vaccine_name') == '乙脑疫苗') & 
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
                (pl.col('vaccine_name') == '乙脑疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=2*12) &
        (pl.col('recommended_seq') == 2) & 
        (pl.col('recommended_vacc') == '乙脑疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

JE_coverage_2=(
    JE_actual_2.join(JE_expected_2,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)


###甲肝
HAV_actual_1=(
    person
    .filter((pl.col('age_month')<18*12) & (pl.col('vaccination_seq')==1))
    .filter(pl.col('vaccine_name')=='甲肝疫苗')
    .filter((pl.col('vaccination_date')>=pl.col('mon_start')) & (pl.col('vaccination_date')<=pl.col('mon_end')))
    .group_by(['vaccination_org', 'vaccine_name','vaccination_seq'])
    .agg(pl.col('id_x').n_unique().alias('vac'))
)

HAV_expected_1=(
    recommendations
    .filter(
        ~pl.col('id_x').is_in(
            person
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '甲肝疫苗') & 
                (pl.col("vaccination_date") <= pl.col('mon_end'))
            )
            ['id_x'].implode()
        )
    )
    .filter(
        (pl.col('age_month') <= 6*12) & 
        (pl.col('age_month')>=18) &
        (pl.col('recommended_seq') == 1) & 
        (pl.col('recommended_vacc') == '甲肝疫苗')
    )
    .filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
    .agg(pl.col('id_x').n_unique().alias('exp'))
)

HAV_coverage_1=(
    HAV_actual_1.join(HAV_expected_1,left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
    right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],how='right')
    .with_columns(pl.col('vac').fill_null(0))
    .with_columns((pl.col('vac')/(pl.col('vac').add(pl.col('exp')))*100).alias('percent'))
)