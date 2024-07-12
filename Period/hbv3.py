# 乙肝3实种
hbv3_shi=(
    jzjl
    .query("age<18 & vaccine_name=='乙肝疫苗' & year_month==@mon_stat & jc==3")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 乙肝3应种
# 筛选出乙肝疫苗且 jzjc 为 2 的记录
id_1=jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 2')['id_x']
vacc_ying0=(
    jzjl
    .query("id_x in @id_1 & age <= 6 & age >= 0 & vaccine_name=='乙肝疫苗' & jc==2")
    .loc[jzjl['vaccination_date']+pd.DateOffset(months=5)<mon_end] # 与第2针满5月
    .loc[jzjl['vaccination_date']+pd.DateOffset(months=5)+pd.DateOffset(months=13)>mon_end] #应种满13次
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 乙肝3已种，但不符合要求
id_2=jzjl.query("vaccine_name == '乙肝疫苗' & jc == 3 & year_month > @mon_stat & age <= 6 & age >= 0")['id_x']

# .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)] 上针13剂次
vacc_ying1=(
    jzjl
    .query("id_x in @id_2 & vaccine_name == '乙肝疫苗' & jc <= 3")
    .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)]
    .groupby('id_x')
    .apply(intervals, mon_stat=mon_stat)
    .reset_index(drop=True)  # 需要重置索引，否则后面可能会出错
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)
