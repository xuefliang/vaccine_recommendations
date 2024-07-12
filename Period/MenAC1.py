# 流脑AC1实种
menac1_shi=(
    jzjl
    .query("(vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==1 & age>=2 & age<18)")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 流脑AC1应种
# 1. 3岁-6岁且已接种A群流脑多糖疫苗第1、2剂，但未接种A群C群流脑多糖疫苗第1剂的儿童数,
# 1702 等2岁之前接种替代疫苗按未种，
# id_ac为A群已种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 2')['id_x']
id_ac=jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 2')['id_x']

# A群已种
id_2=pd.concat([id_ac,id_a],ignore_index=True)

#id_ac2 为AC群未种
id_ac2=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']

id_acs=jzjl.query("vaccination_code in ['1701','1702','1703','1704','5301']")['id_x']
id_acw=jzjl.query("id_x not in @id_acs & age>=3")['id_x']
id_ac2=pd.concat([id_ac2,id_acw],ignore_index=True)

id_3=jzjl.query('id_x in @id_ac2 & id_x in @id_2')['id_x']
menac1_subset1=jzjl.query("id_x in @id_3 & age <= 6 & age >= 3").assign(
    vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 3)
).loc[lambda df: (df['vaccination_date_plus_13_months'] > mon_end)]



# 2. ≥2岁且仅接种A群流脑多糖疫苗第1剂的儿童数（间隔3个月）。
id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x']
id_a=jzjl.query("id_x in @id_a & vacc_months<=24")['id_x']

# id_ac为A群已种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
id_ac=jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']
# A群已种
id_2=pd.concat([id_a,id_ac],ignore_index=True)

#id_ac 为AC群小于2岁接种了一针和AC群接种0
id_ac2=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']
id_acs=jzjl.query("vaccination_code in ['1701','1702','1703','1704','5301']")['id_x']
id_acw=jzjl.query("id_x not in @id_acs & age>=2")['id_x']
id_ac2=pd.concat([id_ac2,id_acw],ignore_index=True)

id_3=jzjl.query('id_x in @id_ac2 & id_x in @id_2')['id_x']
menac1_subset2=jzjl.query("id_x in @id_3 & age >= 2 & vaccine_name in ['流脑疫苗A群','流脑疫苗AC群'] ").assign(
    vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
    vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 13 * 2)
).loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]


# 3 ≥2岁且未接种A群流脑多糖疫苗的儿童数。 
# id_ac为A群未种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']

id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 0')['id_x']

id_3=jzjl.query('id_x in @id_ac & id_x in @id_a')['id_x']
menac1_subset3=jzjl.query("id_x in @id_3 & age >= 2").assign(
    vaccination_date_plus_interval=lambda df: df['birth_date'] + pd.DateOffset(months=12 * 2),
    vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 2)
).loc[lambda df: (df['vaccination_date_plus_interval'] <= mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]


menac1_ying0 = (
    pd.concat([menac1_subset1, menac1_subset2, menac1_subset3], ignore_index=True)
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的1剂次接种
# 流脑AC1应种
# 1. 3岁-6岁且已接种A群流脑多糖疫苗第1、2剂，但未接种A群C群流脑多糖疫苗第1剂的儿童数,
# 1702 等2岁之前接种替代疫苗按未种，
# id_ac为A群已种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 2')['id_x']
id_ac=jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 2')['id_x']

# A群已种
id_2=pd.concat([id_ac,id_a],ignore_index=True)

#id_ac2 为AC群已种
id_ac2=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
id_acs=jzjl.query("id_x in @id_ac2 & age>=3")['id_x']
id_3=jzjl.query('id_x in @id_acs & id_x in @id_2')['id_x']


menac1_subset1=(
    jzjl
    .query("id_x in @id_3 & vaccine_name == '流脑疫苗AC群' & jc == 1 & age<=4")
    .assign(
        vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13+3*12)
    )
    .loc[lambda df: ((df['vaccination_date']>df['vaccination_date_plus_13_months']) & (df['vaccination_date']>mon_end))]
)

# 2. ≥2岁且仅接种A群流脑多糖疫苗第1剂的儿童数（间隔3个月）。
id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x']
id_a=jzjl.query("id_x in @id_a & vacc_months<=24")['id_x']

# id_ac为A群已种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
id_ac=jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']
# A群已种
id_2=pd.concat([id_a,id_ac],ignore_index=True)

#id_ac 为AC群小于2岁接种了一针和AC群接种0
id_ac2=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
id_acs=jzjl.query("id_x in @id_ac2 & age>=3")['id_x']
id_ac2=pd.concat([id_ac2,id_acs],ignore_index=True)

id_3=jzjl.query('id_x in @id_ac2 & id_x in @id_2')['id_x']
menac1_subset2=jzjl.query("id_x in @id_3 & age >= 2 & age<=4 & vaccine_name in ['流脑疫苗A群','流脑疫苗AC群'] ").assign(
    vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
    vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 13 * 2)
).loc[lambda df: ((df['vaccination_date']>df['vaccination_date_plus_13_months']) & (df['vaccination_date']>mon_end))]


# 3 ≥2岁且未接种A群流脑多糖疫苗的儿童数。 
# id_ac为A群未种
id_ac=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']

id_a=jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 0')['id_x']

id_3=jzjl.query('id_x in @id_ac & id_x in @id_a')['id_x']
menac1_subset3=jzjl.query("id_x in @id_3 & age >= 2 & age<=4").assign(
    vaccination_date_plus_interval=lambda df: df['birth_date'] + pd.DateOffset(months=12 * 2),
    vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 2)
).loc[lambda df: ((df['vaccination_date']>df['vaccination_date_plus_13_months']) & (df['vaccination_date']>mon_end))]


menac1_ying1 = (
    pd.concat([menac1_subset1, menac1_subset2, menac1_subset3], ignore_index=True)
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=menac1_ying0.merge(menac1_ying1, on='current_management_code', how='outer')
result=result.merge(menac1_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2))