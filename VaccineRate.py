import pandas as pd
import numpy as np
import janitor
import VaccRate

person2['year_month'] = person2['vaccination_date'].dt.strftime('%Y-%m')
person2['vaccination_org']=person2['vaccination_org'].astype(str)
person2['entry_org']=person2['entry_org'].astype(str)
conditions = [
    person2['vaccination_code'].isin(['0101']),
    person2['vaccination_code'].isin(['0201', '0202', '0203']),
    person2['vaccination_code'].isin(['0301', '0302', '0303','0304','0305','0306','0311','0312','5001']),
    person2['vaccination_code'].isin(['0401', '0402', '0403','4901','5001']),
    person2['vaccination_code'].isin(['1201','1301','1401']),
    person2['vaccination_code'].isin(['0601']),
    person2['vaccination_code'].isin(['1601']),
    person2['vaccination_code'].isin(['1701','1702','1703','1704','5301']),
    person2['vaccination_code'].isin(['1801','1802','1803','1804']),
    person2['vaccination_code'].isin(['1901','1902','1903','2001'])
]
choices = ['乙肝疫苗','卡介苗','脊灰疫苗','百白破疫苗','含麻疹成分疫苗','白破疫苗','流脑疫苗A群','流脑疫苗AC群','乙脑疫苗','甲肝疫苗']
person2['vaccine_name'] = np.select(conditions, choices, default=None)

# current_management_code
# vaccination_org

tmp = person2.query("(year_month == '2021-01') & (vaccination_code=='0101')")
# 实种
(

    tmp
    .groupby(['vaccination_org'])
    .agg(vac=('id_x', 'nunique'))
)

# 每个人接种卡介苗剂次
id_jz=(
    person2
    .groupby(['id_x','vaccination_code'])
    .agg(jc=('id_x', 'size'))
    .reset_index()
    .query("vaccination_code=='0101'")
)

# 计算相差月份数的函数
def calculate_month_difference(date):
    reference_date = pd.to_datetime('2021-01', format='%Y-%m')
    return (reference_date.year-date.year) * 12 +  reference_date.month -date.month

#卡介苗0剂次 小于4岁 应种
tmp2 = (
    person2
    .query("(id_x not in @id_jz.id_x) & (age <= 4)")
    .assign(month=person2['birth_date'].apply(calculate_month_difference))
    .query("0<= month < 13 & age<4")
    .groupby(['current_management_code'])
    .agg(yingzhong=('id_x', 'nunique'))
    .reset_index()
    .query("current_management_code=='333647265032'")
)


# 乙肝
tmp = person2.vaccine.hbv().query("(year_month == '2021-01')")
# mask = tmp['vaccination_org'].isin(['777777777777', '999999999999', '888888888888', '666666666666']) & (tmp['jc'] == 1)
# tmp.loc[mask, 'vaccination_org'] = tmp.loc[mask, 'entry_org']

conditions = [
    tmp['vaccination_org'].isin(['777777777777', '999999999999', '888888888888', '666666666666']) & (tmp['jc'] == 1)
]
tmp['vaccination_org'] = np.select(conditions, [tmp['entry_org']], default=tmp['vaccination_org'])


# 实种
(
    tmp
    .query("vaccination_date>=birth_date & age<18")
    .groupby(['vaccination_org','jc'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
    .query("vaccination_org=='333647265032'")
)


(
    person2
    .query("(id_x not in @id_jz.id_x) & (age <= 6)")
    .assign(month=person2['birth_date'].apply(calculate_month_difference))
    .groupby(['current_management_code'])
    .agg(yingzhong=('id_x', 'nunique'))
    .reset_index()
    .query("current_management_code=='333647265032'")
)

(
    person2
    .query("(id_x not in @id_jz.id_x) & (age < 7)")
    .assign(month=person2['birth_date'].apply(calculate_month_difference))
    .query("0<= month < 13")
    .groupby(['current_management_code'])
    .agg(yingzhong=('id_x', 'nunique'))
    .reset_index()
    .query("current_management_code=='333647265032'")
)


# 每个人接种疫苗的剂次
jzjc= (
    pd.MultiIndex.from_product(
        [person2['id_x'].unique(), 
         ['卡介苗', '乙肝疫苗', '脊灰疫苗', '百白破疫苗', '含麻疹成分疫苗', '白破疫苗', '流脑疫苗A群', '流脑疫苗AC群', '乙脑疫苗', '甲肝疫苗']],
        names=['id_x', 'vaccine_name']
    )
    .to_frame(index=False)
    .merge(
        person2.groupby(['id_x', 'vaccine_name']).agg(jzjc=('id_x', 'size')).reset_index(),
        on=['id_x', 'vaccine_name'],
        how='left'
    )
    .fillna(0)
)

test=(
    jzjc.query("(vaccine_name=='乙肝疫苗') & (jzjc==0)")
)

tmp=(
    person2.query("(age<=6) & (birth_date<='2021-01-31') & (birth_date>='2020-01-01') & (current_management_code=='333647265032') & (id_x in @test.id_x)")
    .drop_duplicates(subset=['id_x'])
)