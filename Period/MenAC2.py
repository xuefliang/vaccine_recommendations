# 流脑AC2实种
menac2_shi=(
    jzjl
    .query("(vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==2 & age>=5 & age<18)")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 流脑AC2应种
# 筛选出流脑AC疫苗且 jzjc 为 1 的记录
id_1=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
menac2_ying0 = (
    jzjl
    .query("id_x in @id_1 & vacc_months >= 3*12 & vaccine_name == '流脑疫苗AC群' & jc == 1 ")
    .query("year_month < @mon_stat")
    .assign(
        vaccination_date_plus=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3),  # 与第1针满3岁
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3+13)  # 应种满13次
    )
    .loc[lambda df: (df['vaccination_date_plus'] < mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 流脑AC2已种，但不符合要求

# 流脑AC2已种，但不符合要求
id_2=jzjl.query("vaccine_name == '流脑疫苗AC群' & jc == 2 & year_month > @mon_stat & vacc_months >= 12*3")['id_x']
menac2_ying1=(
    jzjl
    .query("id_x in @id_2 & vaccine_name == '流脑疫苗AC群' & jc == 1")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+12*3)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=menac2_ying0.merge(menac2_ying1, on='current_management_code', how='outer')
result=result.merge(menac2_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
