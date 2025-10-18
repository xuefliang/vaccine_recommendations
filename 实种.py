import polars as pl


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
    .filter(
        ~pl.col('id_x').is_in(
            person
            .with_columns(
                pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
                pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end')
            )
            .filter(
                (pl.col('vaccination_seq') == 1) & 
                (pl.col('vaccine_name') == '卡介苗') & 
                (pl.col("vaccination_date") < pl.col('mon_start'))
            )
            ['id_x'].implode()
        )
    )
    .with_columns(
        pl.lit('2021-01-15').str.to_date().dt.month_start().alias('mon_start'),
        pl.lit('2021-01-15').str.to_date().dt.month_end().alias('mon_end')
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

