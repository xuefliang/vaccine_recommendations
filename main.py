import sys
import os
import pandas as pd
import numpy as np
from datetime import timedelta
import janitor

person2=pd.read_pickle('/mnt/d/标准库接种率/data/person2.pkl')
# jzjl=pd.read_pickle('/mnt/d/标准库接种率/data/jzjl.pkl')

# person=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard.csv',dtype={'CURRENT_MANAGEMENT_CODE':str}).clean_names()
# person__vaccination=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard_vaccination.csv',dtype={'VACCINATION_CODE':str}).clean_names()
# person2=person.merge(person__vaccination,how='left',left_on='id',right_on='person_id')

#统计日期
mon_stat='2020-12'
mon_end=pd.date_range(mon_stat, periods=1, freq='M')[0]

person2['birth_date'] = pd.to_datetime(person2['birth_date'], format='%Y%m%dT%H%M%S')
person2['vaccination_date'] = pd.to_datetime(person2['vaccination_date'], format='%Y%m%dT%H%M%S')
person2['vaccination_org']=person2['vaccination_org'].astype(str)
person2['entry_org']=person2['entry_org'].astype(str)
#计算年龄、月龄、接种年月、接种月龄
person2['age'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - person2['birth_date']).dt.days / 365.25).astype(int)
person2['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - person2['birth_date']).dt.days / 30.44).astype(int)
person2['year_month'] = person2['vaccination_date'].dt.strftime('%Y-%m')
person2['vacc_months'] = np.floor((person2['vaccination_date'] - person2['birth_date']).dt.days / 30.44).astype(int)

from script.Vaccine import *
# 出生队列接种率
result=person2.vaccine.cohort_rate(by_age=True)

# 时段接种率
from script.Vaccine import *
result = person2.vaccine.duration_rate(mon_stat)


# from vaccineanalysis import *
# from Period.common import *
# jzjl = person2.vaccineanalysis.all_vaccines()
# jzjc=get_immuhis(jzjl)
# jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
# jzjl['vacc_months'] = np.floor((jzjl['vaccination_date'] - jzjl['birth_date']).dt.days / 30.44).astype(int)

# 乙肝疫苗1和卡介苗1补录按实种
# condition = (jzjl['vaccination_org'].isin(['777777777777', '888888888888', '999999999999'])) & \
#             (jzjl['vaccine_name'].isin(['乙肝疫苗', '卡介苗'])) & \
#             (jzjl['jc'] == 1)

# jzjl.loc[condition, 'vaccination_org'] = jzjl['entry_org']

# 计算免疫史，每个人接种疫苗
# jzjc= (
#     pd.MultiIndex.from_product(
#         [jzjl['id_x'].unique(), 
#          ['卡介苗', '乙肝疫苗', '脊灰疫苗', '百白破疫苗', '含麻疹成分疫苗', '白破疫苗', '流脑疫苗A群', '流脑疫苗AC群', '乙脑疫苗', '甲肝疫苗']],
#         names=['id_x', 'vaccine_name']
#     )
#     .to_frame(index=False)
#     .merge(
#         jzjl.groupby(['id_x','vaccine_name']).agg(jzjc=('id_x', 'size')).reset_index(),
#         on=['id_x', 'vaccine_name'],
#         how='left'
#     )
#     .fillna(0)
#     .astype({'jzjc': 'int'})
# )

# print("Current working directory:", os.getcwd())

# project_dir = '/mnt/d/标准库接种率/'
# if os.getcwd() != project_dir:
#     os.chdir(project_dir)

# for path in sys.path:
#     print(path)

# from Period import BCG, HBV, DPT, PV, HepA, JEV, MCV, MenA, MenAC, TD
# # 定义需要调用的函数列表
# functions = [
#     BCG.BCG,
#     HBV.HBV1, HBV.HBV2, HBV.HBV3,
#     DPT.DPT1, DPT.DPT2, DPT.DPT3, DPT.DPT4,
#     PV.PV1, PV.PV2, PV.PV3, PV.PV4,
#     MCV.MCV1, MCV.MCV2,
#     HepA.HepA,
#     JEV.JEV1, JEV.JEV2,
#     MenA.MenA1, MenA.MenA2,
#     MenAC.MenAC1, MenAC.MenAC2,
#     # TD.TD
# ]
# # 使用列表推导式调用函数并生成结果列表
# results = [func(jzjl, person2, jzjc, mon_stat, mon_end) for func in functions]
# result = pd.concat(results)

# from scipy.stats import binomtest
# def calculate_ci(row, confidence_level=0.95):
#     k = int(row['actual_count'])
#     n = int(row['actual_count'] + row['expected_count'] + row['invalid_count'])
#     bino = binomtest(k=k, n=n, p=0.05, alternative='two-sided')
#     ci = bino.proportion_ci(confidence_level=confidence_level)
#     return pd.Series([ci.low * 100, ci.high * 100], index=['lower_ci', 'upper_ci'])

# result[['lower_ci', 'upper_ci']] = result.apply(calculate_ci, axis=1, confidence_level=0.95)