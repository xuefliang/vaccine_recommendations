# 卡介苗1实种
vacc_shi=(
    jzjl
    .query("age<4 & vaccine_name=='卡介苗' & year_month==@mon_stat & jc==1")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 卡介苗1应种
# 筛选出乙肝疫苗且 jzjc 为 0 的记录
id_0=jzjc.query('vaccine_name == "卡介苗" & jzjc == 0')['id_x']
vacc_ying0=(
    person2
    .query("id_x in @id_0 & age < 4 & age >= 0")
    .loc[mon_end < person2['birth_date'] + pd.DateOffset(months=13)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的1剂次接种
vacc_ying1=(
    jzjl
    .query("vaccine_name == '卡介苗' & jc == 1 & year_month > @mon_stat & age < 4 & age >= 0")
    .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=vacc_ying0.merge(vacc_ying1, on='current_management_code', how='outer')
result=result.merge(vacc_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2),
                     vaccine_name='卡介苗1')
