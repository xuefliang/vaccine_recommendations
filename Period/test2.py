yingzhong=(
    pd.read_excel("/mnt/d/标准库接种率/标准库国家计算结果（默认版本）/机构数据334878619388/期间接种率/接种率明细数据/334878619388-2021-01-01 00_00_00-2021-01-31 23_59_59 期间接种率明细.xlsx",sheet_name='A群C群2')
    .query("index>=14 & 接种时间.notnull()")
)

id_1=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
menac2_ying0=(
    jzjl
    .query("id_x in @id_1 & vacc_months >= 5*12 & vaccine_name=='流脑疫苗AC群' & jc==1")
    .query("year_month<@mon_stat")
    .loc[jzjl['vaccination_date']+pd.DateOffset(months=12*3)<mon_end] # 与第1针满3岁
    .loc[jzjl['vaccination_date']+pd.DateOffset(months=12*3)+pd.DateOffset(months=13)>mon_end] #应种满13次
    .query("current_management_code=='334878619388'")
)

menac2_ying0 = (
    jzjl
    .query("id_x in @id_1 & vacc_months >= 3*12 & vaccine_name == '流脑疫苗AC群' & jc == 1 ")
    .query("year_month < @mon_stat")
    .assign(
        vaccination_date_plus=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3),  # 与第1针满3岁
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3+13)  # 应种满13次
    )
    .loc[lambda df: (df['vaccination_date_plus'] < mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .query("current_management_code == '334878619388'")
)

id_1=jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc >= 1')['id_x']
menac2_ying0=(
    jzjl
    .query("id_x in @id_1 & vacc_months >= 3*12 & vaccine_name == '流脑疫苗AC群'")
    .query("year_month < @mon_stat")
    .groupby(['id_x'])
    .apply(lambda x: x.nsmallest(1, 'jc'))
    .reset_index(drop=True)
    .assign(
        vaccination_date_plus=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3),  # 与第1针满3岁
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3+13)  # 应种满13次
    )
    .loc[lambda df: (df['vaccination_date_plus'] < mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .query("current_management_code == '334878619388'")
)

menac2_ying0 = (
    jzjl
    .query("id_x in @id_1 & vacc_months >= 3*12 & vaccine_name == '流脑疫苗AC群'")
    .query("year_month < @mon_stat")
    .groupby(['id_x'])
    .apply(lambda x: x.nlargest(1, 'jc'))
    .reset_index(drop=True)
    .assign(
        vaccination_date_plus=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3),  # 与第1针满3岁
        vaccination_date_plus_13_months=lambda df: df['vaccination_date'] + pd.DateOffset(months=12*3+13)  # 应种满13次
    )
    .loc[lambda df: (df['vaccination_date_plus'] < mon_end) & (df['vaccination_date_plus_13_months'] > mon_end)]
    .query("current_management_code == '334878619388'")
)

menac2_ying0 = (
    jzjl
    .query("id_x in @id_1 & vacc_months >= 5*12 & vaccine_name == '流脑疫苗AC群' & jc == 1")
    .query("year_month < @mon_stat")
    .query("current_management_code == '334878619388'")
)

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
    .query("current_management_code == '334878619388'")
)


tmp2=menac2_ying1[~menac2_ying1['id_x'].isin(yingzhong['个案ID'])]

tmp2=jzjl[jzjl['id_x'].isin(tmp2['id_x'])]

tmp3=yingzhong[~yingzhong['个案ID'].isin(menac2_ying1['id_x'])]

tmp3=jzjl[jzjl['id_x'].isin(yingzhong['个案ID'])]