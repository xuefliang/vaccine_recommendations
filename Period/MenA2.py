mena2_shi=(
    jzjl
    .query("(age<2 & vaccine_name=='流脑疫苗A群' & year_month==@mon_stat & jc==2) | (age<2 & vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==2)")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 流脑A1应种
# 筛选出流脑A1疫苗且 jzjc 为 0 的记录,需要增加疫苗编码1702的排除条件。
id_1=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x']
mena_id=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1') ['id_x']
mena_id=jzjl.query(" id_x in @mena_id & months<=23")['id_x']
mena2_ying0=(
    jzjl
    .query("(id_x in @id_1 & months <=23 & months>=9 & year_month<@mon_stat & jc==1 & vaccine_name =='流脑疫苗A群')|(id_x in @mena_id & months <=23 & months>=9 & year_month<@mon_stat & vaccine_name =='流脑疫苗AC群' & jc==1)")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+3)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的2剂次接种
id_2=jzjl.query("vaccine_name == '流脑疫苗A群' & jc == 2 & year_month > @mon_stat & months <=23 & months>=9")['id_x']
id_3=jzjl.query("vaccine_name == '流脑疫苗AC群' & jc == 2 & year_month > @mon_stat & months <=23 & months>=9")['id_x']

mena2_ying1=(
    jzjl
    .query("(id_x in @id_2 & vaccine_name == '流脑疫苗A群' & jc == 1) | (id_x in @id_3 & vaccine_name == '流脑疫苗AC群' & jc == 1)")
    .assign(
        vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=13+3)
    )
    .loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=mena2_ying0.merge(mena2_ying1, on='current_management_code', how='outer')
result=result.merge(mena2_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
