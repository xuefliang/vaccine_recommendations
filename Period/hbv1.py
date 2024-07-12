import pandas as pd
import numpy as np
from datetime import timedelta
import janitor

person2=pd.read_pickle('/mnt/d/标准库接种率/person2.pkl')
jzjl=pd.read_pickle('/mnt/d/标准库接种率/jzjl.pkl')

mon_stat='2021-01'
mon_end=pd.date_range(mon_stat, periods=1, freq='M')[0]

person2['birth_date'] = pd.to_datetime(person2['birth_date'], format='%Y%m%dT%H%M%S')
person2['vaccination_date'] = pd.to_datetime(person2['vaccination_date'], format='%Y%m%dT%H%M%S')
person2['age'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - person2['birth_date']).dt.days / 365.25).astype(int)
person2['year_month'] = person2['vaccination_date'].dt.strftime('%Y-%m')
person2['vaccination_org']=person2['vaccination_org'].astype(str)
person2['entry_org']=person2['entry_org'].astype(str)
jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
person2['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - person2['birth_date']).dt.days / 30.44).astype(int)
jzjl['vacc_months'] = np.floor((jzjl['vaccination_date'] - jzjl['birth_date']).dt.days / 30.44).astype(int)

# jzjl = person2.vaccine.all_vaccines()

# 乙肝疫苗1和卡介苗1补录按实种
condition = (jzjl['vaccination_org'].isin(['777777777777', '888888888888', '999999999999'])) & \
            (jzjl['vaccine_name'].isin(['乙肝疫苗', '卡介苗'])) & \
            (jzjl['jc'] == 1)

jzjl.loc[condition, 'vaccination_org'] = jzjl['entry_org']

# 每个人接种疫苗的剂次
jzjc= (
    pd.MultiIndex.from_product(
        [jzjl['id_x'].unique(), 
         ['卡介苗', '乙肝疫苗', '脊灰疫苗', '百白破疫苗', '含麻疹成分疫苗', '白破疫苗', '流脑疫苗A群', '流脑疫苗AC群', '乙脑疫苗', '甲肝疫苗']],
        names=['id_x', 'vaccine_name']
    )
    .to_frame(index=False)
    .merge(
        jzjl.groupby(['id_x','vaccine_name']).agg(jzjc=('id_x', 'size')).reset_index(),
        on=['id_x', 'vaccine_name'],
        how='left'
    )
    .fillna(0)
    .astype({'jzjc': 'int'})
)


# 乙肝1实种
hbv1_shi=(
    jzjl
    .query("age<18 & vaccine_name=='乙肝疫苗' & year_month==@mon_stat & jc==1")
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

# 乙肝1应种
# 筛选出乙肝疫苗且 jzjc 为 0 的记录
id_0=jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 0')['id_x']
hbv1_ying0=(
    person2
    .query("id_x in @id_0 & age <= 6 & age >= 0")
    .loc[mon_end < person2['birth_date'] + pd.DateOffset(months=13)]
    .groupby(['current_management_code'])
    .agg(ying0=('id_x', 'nunique'))
    .reset_index()
)

# 不符合要求的1剂次接种
hbv1_ying1=(
    jzjl
    .query("vaccine_name == '乙肝疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & age >= 0")
    .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)]
    .groupby(['current_management_code'])
    .agg(ying1=('id_x', 'nunique'))
    .reset_index()
)

result=hbv1_ying0.merge(hbv1_ying1, on='current_management_code', how='outer')
result=result.merge(hbv1_shi, left_on='current_management_code',right_on='vaccination_org', how='outer')
result.fillna(0, inplace=True)
result=result.assign(prop=lambda x: (x['vac'] / (x['vac'] + x['ying0'] + x['ying1']) * 100).round(2),
                     vaccine_name='乙肝疫苗1')
