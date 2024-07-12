# 脊灰1实种
jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
pv1_shi=(
    jzjl
    .query("age<=18 & vaccine_name=='脊灰疫苗' & year_month==@mon_stat & jc==1")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 脊灰1应种
# 筛选出脊灰疫苗且 jzjc 为 0 的记录
id_0=jzjc.query('vaccine_name == "脊灰疫苗" & jzjc == 0')['id_x']
pv1_ying0=(
    person2
    .query("id_x in @id_0 & age <= 6 & months>=2")
    .assign(
        vaccination_date_plus=lambda df: df['birth_date'] + pd.DateOffset(months=13+2)
    )
    .loc[lambda df: df['vaccination_date_plus'] > mon_end]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的1剂次接种
pv1_ying1=(
    jzjl
    .query("vaccine_name == '脊灰疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & months>=2")
    .assign(
        vaccination_date_plus=lambda df: df['birth_date'] + pd.DateOffset(months=13+2)
    )
    .loc[lambda df: df['vaccination_date_plus'] > mon_end]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=pv1_ying0.merge(pv1_ying1, on='current_management_code', how='outer')
result=result.merge(pv1_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
