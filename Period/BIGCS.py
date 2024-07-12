import pandas as pd
import numpy as np
import janitor
import math

# person=pd.read_csv('/mnt/d/标准库接种率 v1.0.8-20230720/标准库数据/person_standard.csv',dtype={'CURRENT_MANAGEMENT_CODE':str},parse_dates=['BIRTH_DATE']).clean_names()
person=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard.csv',dtype={'CURRENT_MANAGEMENT_CODE':str}).clean_names()
person__vaccination=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard_vaccination.csv',dtype={'VACCINATION_CODE':str}).clean_names()

person=(
    person
    .query("current_management_code=='333647265032'")
    .assign(tj_date=lambda x: pd.to_datetime('2021-12-31', format='%Y-%m-%d'),
            birth_date=lambda x: pd.to_datetime(x.birth_date.str.split('T').str[0], format='%Y%m%d'),
            age=lambda x: np.floor((x.tj_date - x.birth_date).dt.days / 365.25).astype(int)
            )
    .query("age>=1 & age<=18")
    .merge(person__vaccination,how='left',left_on='id',right_on='person_id')
)

# person = (
#     person.query("current_management_code=='333647265032'")
#     .assign(
#         tj_date=lambda x: pd.to_datetime('2021-12-31', format='%Y-%m-%d'),
#         birth_date=lambda x: pd.to_datetime(x['birth_date'].astype(str).str.split('T').str[0], format='%Y%m%d'),
#         age=lambda x: x.apply(
#             lambda row: row['tj_date'].year - row['birth_date'].year - ((row['tj_date'].month, row['tj_date'].day) < (row['birth_date'].month, row['birth_date'].day)),
#             axis=1
#         )
#     )
# )

(
    person
    .groupby('age')
    .agg(cnt=('id_x', 'nunique'),
        vac=('id_x', lambda x: x[(person.loc[x.index, 'vaccination_code'] == '0101') & (person.loc[x.index, 'vaccination_seq'] == 1)].nunique())
        )
    .reset_index()
)

(
    person
    .query("current_management_code=='333647265032'")
    .query("vaccination_code in ['0201','0202','0203']")
    .assign(grp=lambda x: x['vaccination_code'].str.slice(0,2))
    .groupby(['id_x','grp'])
    .agg(cnt=('id_x', 'size'))
    .reset_index()
)

( 
person2.query("vaccination_code in ['0201','0202','0203'] & id_x=='0004bc0c2c8941058bd2a81fc01b5335'")
)


( 
    result.groupby(['age','jc']).agg(cnt=('id_x', 'nunique'))
)

def calculate_vaccine_rate(tmp, person):
    fenmu = (
        person
        .query("age>=1")
        .groupby(['current_management_code', 'age'])
        .agg(cnt=('id_x', 'nunique'))
        .reset_index()
    )

    fenzi = (
        tmp
        .query("age>=1")
        .groupby(['current_management_code', 'age', 'vaccine_name', 'jc'])
        .agg(vac=('id_x', 'nunique'))
        .reset_index()
    )

    result = fenzi.merge(fenmu, on=['current_management_code', 'age'], how='inner')
    result.to_excel('/mnt/c/Users/xuefliang/Downloads/vaccine_rate.xlsx', index=False)

    return result

# 调用函数并传入参数
tmp = pd.concat([bcg, hbv], ignore_index=True)
person = person2
vaccine_rate = calculate_vaccine_rate(tmp, person)


fenmu=(
    person2
    .query("age>=1")
    .groupby(['current_management_code','age'])
    .agg(cnt=('id_x', 'nunique'))
    .reset_index()
)

fenzi=(
    tmp
    .query("age>=1")
    .groupby(['current_management_code','age','vaccine_name','jc'])
    .agg(vac=('id_x', 'nunique'))
    .reset_index()
)

fenzi.merge(fenmu,on=['current_management_code','age'],how='inner').to_excel('/mnt/c/Users/xuefliang/Downloads/vaccine_rate.xlsx',index=False)


def calculate_vaccine_rate(tmp, person):
    fenmu = (
        person
        .query("age>=1")
        .groupby(['current_management_code', 'age'])
        .agg(cnt=('id_x', 'nunique'))
        .reset_index()
    )

    fenzi = (
        tmp
        .query("age>=1")
        .groupby(['current_management_code', 'age', 'vaccine_name', 'jc'])
        .agg(vac=('id_x', 'nunique'))
        .reset_index()
    )

    return fenzi.merge(fenmu, on=['current_management_code', 'age'], how='inner')

calculate_vaccine_rate(tmp, person2)

def rm(*args):
    # 获取当前的全局变量字典
    global_vars = globals()
    
    for var in args:
        # 尝试从全局变量中删除
        if var in global_vars:
            del global_vars[var]
        else:
            print(f"Variable '{var}' not found.")

rm('tmp','person','vaccine_rate')