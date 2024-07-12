# 白破1实种
jzjl['months'] = np.floor((pd.to_datetime(
    mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
td1_shi = (
    jzjl
    .query("age<12 & vaccine_name=='白破疫苗' & year_month==@mon_stat & jc==1")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 白破1应种
# 筛选出白破1疫苗且 jzjc 为 0 的记录
id_1 = jzjc.query('vaccine_name == "白破疫苗" & jzjc == 0')['id_x']
td1_ying0 = (
    person2
    .query("id_x in @id_1 & age <= 11 & age>=6")
    .assign(
        vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(years=6) +
        pd.DateOffset(months=13)
    )
    .loc[lambda df: (df['vaccination_date_plus_13_months'] > mon_end)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的1剂次接种
id_2 = jzjl.query(
    "vaccine_name == '白破疫苗' & jc == 1 & year_month > @mon_stat & age <= 11 & age>=6")['id_x']

#和百白破疫苗间隔不足
jiange= (
    jzjl[jzjl['vaccine_name'].isin(['百白破疫苗', '白破疫苗'])]
    .groupby('id_x')
    .filter(lambda x: (
        x[(x['vaccine_name'] == '百白破疫苗') & (x['jc'] >= 3)].shape[0] > 0 and
        x[(x['vaccine_name'] == '白破疫苗') & (x['jc'] == 1)].shape[0] > 0
    ))
    .groupby('id_x')
    .apply(lambda group: (
        group.assign(
            max_bbp_date=group.loc[group['vaccine_name']
                                   == '百白破疫苗', 'vaccination_date'].max(),
            jc_value=group.loc[group['vaccine_name'] == '百白破疫苗', 'jc'].max()
        )
        .query("vaccine_name == '白破疫苗'")
        .assign(
            interval_months=lambda x: (
                x['vaccination_date'] - x['max_bbp_date']) / timedelta(days=30)
        )
        .query("(jc_value == 4 and interval_months < 12) or (jc_value == 3 and interval_months < 6)")
    ))
    .reset_index(drop=True)
)

td1_ying1 = (
    jzjl
    .query("id_x in @id_2 & vaccine_name == '白破疫苗' & jc == 1")
    .assign(
        vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(years=6) +
        pd.DateOffset(months=13)
    )
    .loc[lambda df: (df['vaccination_date_plus_13_months'] > mon_end)]
)

#合并间隔不足的
td1_ying1=(
    pd.concat([jiange[['id_x', 'current_management_code']], td1_ying1[['id_x', 'current_management_code']]])
    .groupby('current_management_code')
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result = td1_ying0.merge(td1_ying1, on='current_management_code', how='outer')
result = result.merge(td1_shi, left_on='current_management_code',
                      right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result = result.assign(prop=lambda x: (
    x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))
