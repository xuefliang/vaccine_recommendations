# 百白破2实种
# jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
dpt2_shi=(
    jzjl
    .query("age<6 & vaccine_name=='百白破疫苗' & year_month==@mon_stat & jc==2")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 百白破2应种
# 筛选出百白破疫苗且 jzjc 为 1 的记录
id_1=jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 1')['id_x']
dpt2_ying0=(
    jzjl
    .query("id_x in @id_1 & age <=5 & months>=4 & vaccine_name=='百白破疫苗' & year_month<@mon_stat & jc==1")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=1),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+1)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的2剂次接种
id_2=jzjl.query("vaccine_name == '百白破疫苗' & jc == 2 & year_month > @mon_stat & age <=5 & months>=4")['id_x']

dpt2_ying1=(
    jzjl
    .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 1")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=1),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+1)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=dpt2_ying0.merge(dpt2_ying1, on='current_management_code', how='outer')
result=result.merge(dpt2_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
