import sys
import os
import pandas as pd
import numpy as np
from datetime import timedelta
import janitor
from script.common import *

person2=pd.read_pickle('/mnt/d/标准库接种率/data/person2.pkl')
person2=person2.query("current_management_code=='334878619388' | vaccination_org=='334878619388'")
# # jzjl=pd.read_pickle('/mnt/d/标准库接种率/data/jzjl.pkl')

# # person=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard.csv',dtype={'CURRENT_MANAGEMENT_CODE':str}).clean_names()
# # person__vaccination=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard_vaccination.csv',dtype={'VACCINATION_CODE':str}).clean_names()
# # person2=person.merge(person__vaccination,how='left',left_on='id',right_on='person_id')

#统计日期
mon_stat='2021-01'
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
person2=replace(person2)

# jzjl['vacc_months'] = np.floor((jzjl['vaccination_date'] - jzjl['birth_date']).dt.days / 30.44).astype(int)
# jzjl['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jzjl['birth_date']).dt.days / 30.44).astype(int)
# from script.Vaccine import *
# # 出生队列接种率
# cohort_result=person2.vaccine.cohort_rate(by_age=True)
# cohort_result[['lower_ci', 'upper_ci']] = cohort_result.apply(
#     lambda row: calculate_ci(row['vac'],row['cnt'], confidence_level=0.95), axis=1
# )

# # 时段接种率
# from script.Vaccine import *
# from script.common import *
# # test=person2.vaccine.MenA1(mon_stat)
# duration_result = person2.vaccine.duration_rate(mon_stat)
# duration_result[['lower_ci', 'upper_ci']] = duration_result.apply(
#     lambda row: calculate_ci(int(row['actual_count']),int(row['actual_count'] + row['expected_count'] + row['invalid_count']), confidence_level=0.95), axis=1
# )



# # print("Current working directory:", os.getcwd())

# # project_dir = '/mnt/d/标准库接种率/'
# # if os.getcwd() != project_dir:
# #     os.chdir(project_dir)

# # for path in sys.path:
# #     print(path)

# jz = (
#     pd.read_csv('/mnt/d/标准库接种率/data/jiezhongmingxi.csv', dtype=str)
#     .clean_names()
#     .rename(columns={
#         'grda_code': 'id_x',
#         'csrq': 'birth_date',
#         'gldw_bm': 'current_management_code',
#         'ym_bm': 'vaccination_code',
#         'jz_sj': 'vaccination_date',
#         'jzdd_dm': 'vaccination_org',
#         'xt_djjgdm': 'entry_org'
#     })
# )

# jz['birth_date'] = pd.to_datetime(jz['birth_date'], format='%Y/%m/%d',errors='coerce')
# jz['vaccination_date'] = pd.to_datetime(jz['vaccination_date'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
# jz=(
#     jz.query("vaccination_date>=birth_date & birth_date.notnull() & vaccination_date.notnull()")
# )
# #计算年龄、月龄、接种年月、接种月龄
# jz['age'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jz['birth_date']).dt.days / 365.25).astype(int)
# jz['months'] = np.floor((pd.to_datetime(mon_end, format='%Y-%m-%d') - jz['birth_date']).dt.days / 30.44).astype(int)
# jz['year_month'] = jz['vaccination_date'].dt.strftime('%Y-%m')
# jz['vacc_months'] = np.floor((jz['vaccination_date'] - jz['birth_date']).dt.days / 30.44)
# jz['vacc_months'] = jz['vacc_months'].astype(int)

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


######################################################################
#统计日期
# mon_stat='2024-06'
# mon_end=pd.date_range(mon_stat, periods=1, freq='ME')[0]
# jz = (
#     pd.read_csv('/mnt/d/标准库接种率/data/jiezhongmingxi.csv', dtype=str)
#     .clean_names()
#     .rename(columns={
#         'grda_code': 'id_x',
#         'csrq': 'birth_date',
#         'gldw_bm': 'current_management_code',
#         'ym_bm': 'vaccination_code',
#         'jz_sj': 'vaccination_date',
#         'jzdd_dm': 'vaccination_org',
#         'xt_djjgdm': 'entry_org'
#     })
#     .assign(
#         birth_date=lambda df: pd.to_datetime(df['birth_date'], format='%Y/%m/%d', errors='coerce'),
#         vaccination_date=lambda df: pd.to_datetime(df['vaccination_date'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
#     )
#     .query("vaccination_date >= birth_date & birth_date.notnull() & vaccination_date.notnull()")
#     .assign(
#         age=lambda df: np.floor((mon_end - df['birth_date']).dt.days / 365.25).astype(int),
#         months=lambda df: np.floor((mon_end - df['birth_date']).dt.days / 30.44).astype(int),
#         year_month=lambda df: df['vaccination_date'].dt.strftime('%Y-%m'),
#         vacc_months=lambda df: np.floor((df['vaccination_date'] - df['birth_date']).dt.days / 30.44).astype(int)
#     )
# )

# from script.Vaccine import *
# # 出生队列接种率
# cohort_result=jz.vaccine.cohort_rate(by_age=True)
# cohort_result[['lower_ci', 'upper_ci']] = cohort_result.apply(
#     lambda row: calculate_ci(row['vac'],row['cnt'], confidence_level=0.95), axis=1
# )

# 时段接种率
from script.Vaccine import *
from script.common import *

# test = person2.iloc[0:1000, person2.columns.get_indexer(['id_x','patient_name', 'birth_date','current_management_code',
# 'vaccination_code', 'vaccination_date', 'vaccination_seq',
# 'vaccination_org', 'entry_org','entry_date', 'age', 'months',
# 'year_month', 'vacc_months'])]

duration_result = person2.vaccine.duration_rate(mon_stat)
duration_result[['lower_ci', 'upper_ci']] = duration_result.apply(
    lambda row: calculate_ci(int(row['actual_count']),int(row['actual_count'] + row['expected_count'] + row['invalid_count']), confidence_level=0.95), axis=1
)