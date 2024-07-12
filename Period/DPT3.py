# 百白破3实种
# jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
dpt3_shi=(
    jzjl
    .query("age<6 & vaccine_name=='百白破疫苗' & year_month==@mon_stat & jc==3")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 百白破3应种
# 筛选出百白破疫苗且 jzjc 为 2 的记录
id_1=jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 2')['id_x']
dpt3_ying0=(
    jzjl
    .query("id_x in @id_1 & age <= 5 & months>=5 & vaccine_name=='百白破疫苗' & year_month<@mon_stat & jc == 2")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=1),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+1)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的3剂次接种
id_2=jzjl.query("vaccine_name == '百白破疫苗' & jc == 3 & year_month > @mon_stat & age <= 5 & months>=5")['id_x']

dpt3_ying1=(
    jzjl
    .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 2")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=1),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+1)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=dpt3_ying0.merge(dpt3_ying1, on='current_management_code', how='outer')
result=result.merge(dpt3_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
