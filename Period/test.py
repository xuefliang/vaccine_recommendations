
yingzhong=(
    pd.read_excel("/mnt/d/标准库接种率/标准库国家计算结果（默认版本）/机构数据334878619388/期间接种率/接种率明细数据/334878619388-2021-01-01 00_00_00-2021-01-31 23_59_59 期间接种率明细.xlsx",sheet_name='A群C群1')
    .query("index>=23 & 接种时间.notnull()")
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
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

tmp2=menac1_subset3[~menac1_subset2['id_x'].isin(yingzhong['个案ID'])]

tmp2=jzjl[jzjl['id_x'].isin(tmp2['id_x'])]

tmp3=yingzhong[~yingzhong['个案ID'].isin(menac1_ying1['id_x'])]

tmp1=jzjl[jzjl['id_x'].isin(tmp3['个案ID'])]

tmp4=jzjl[jzjl['id_x'].isin(yingzhong['个案ID'])]

tmp3.to_excel("/mnt/d/tmp1.xlsx", index=False)




