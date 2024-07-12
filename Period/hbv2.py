# 乙肝2实种
vacc_shi=(
    jzjl
    .query("age<18 & vaccine_name=='乙肝疫苗' & year_month==@mon_stat & jc==2")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 乙肝2应种
# 筛选出乙肝疫苗且 jzjc 为 1 的记录
id_1=jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 1')['id_x']
vacc_ying0=(
    jzjl
    .query("id_x in @id_1 & age <= 6 & age >= 0 & vaccine_name=='乙肝疫苗'")
    .query("year_month<@mon_stat")
    .loc[mon_end < jzjl['vaccination_date'] + pd.DateOffset(months=13)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的2剂次接种
def intervals(group, mon_stat):
    # 将 year_month 转换为日期格式
    group['year_month'] = pd.to_datetime(group['year_month'], format='%Y-%m')
    mon_stat = pd.to_datetime(mon_stat, format='%Y-%m')
    
    # 条件1: jc 等于 1 的 year_month 都小于 mon_stat
    condition_1 = (group[group['jc'] == 1]['year_month'] < mon_stat).all()

    condition_3 = (group[group['jc'] == 2]['year_month'] > mon_stat).all()

    # 条件2: jc 等于 2 的 year_month 都大于 jc 等于 1 的 year_month + 1 个月
    if not group[group['jc'] == 1].empty:
        condition_2 = (group[group['jc'] == 2]['year_month'] > group[group['jc'] == 1]['year_month'].max() + pd.DateOffset(months=1)).all()
    else:
        condition_2 = False
        
    # 如果两个条件都满足，返回 group，否则返回空的 DataFrame
    if condition_1 and condition_2 and condition_3 :
        return group
    return pd.DataFrame()

id_2=(
    jzjl
    .query("vaccine_name == '乙肝疫苗' & jc == 2 & year_month > @mon_stat & age <= 6 & age >= 0")
    ['id_x']
    .values
)


vacc_ying1=(
    jzjl
    .query("id_x in @id_2 & vaccine_name == '乙肝疫苗' & jc <= 2")
    .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)]
    .groupby('id_x')
    .apply(intervals, mon_stat=mon_stat)
    .reset_index(drop=True)  # 需要重置索引，否则后面可能会出错
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=vacc_ying0.merge(vacc_ying1, on='current_management_code', how='outer')
result=result.merge(vacc_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2),
                     vaccine_name='乙肝疫苗2')