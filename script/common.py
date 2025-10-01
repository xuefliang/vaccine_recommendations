import pandas as pd
import numpy as np
from datetime import timedelta
from scipy.stats import binomtest

def add_13_months(df, date_column,months=0):
    return df.assign(plus_13_months=lambda x: x[date_column] + pd.DateOffset(months=13) + pd.DateOffset(months=months))

def add_intervals(df, date_column, months):
    return df.assign(add_intervals=lambda x: pd.to_datetime(x[date_column]) + pd.DateOffset(months=months))

# 不符合要求的2剂次接种
def intervals(group, mon_stat):
    group['year_month'] = pd.to_datetime(group['year_month'], format='%Y-%m')
    mon_stat = pd.to_datetime(mon_stat, format='%Y-%m')
    
    condition_1 = (group[group['jc'] == 1]['year_month'] < mon_stat).all()
    condition_3 = (group[group['jc'] == 2]['year_month'] > mon_stat).all()

    if not group[group['jc'] == 1].empty:
        condition_2 = (group[group['jc'] == 2]['year_month'] > group[group['jc'] == 1]['year_month'].max() + pd.DateOffset(months=1)).all()
    else:
        condition_2 = False
        
    if condition_1 and condition_2 and condition_3:
        return group
    return pd.DataFrame()

def calculate_vaccine_proportion(expected, actual, invalid, vaccine_name):
    """
    计算疫苗接种率并返回结果数据框。

    参数：
    expected (pd.DataFrame): 预期数据框。
    actual (pd.DataFrame): 实际数据框。
    invalid (pd.DataFrame): 无效数据框。
    vaccine_name (str): 疫苗名称。

    返回：
    pd.DataFrame: 包含接种率的结果数据框。
    """
    result = (
        expected
        .merge(invalid, on='current_management_code', how='outer')
        .merge(actual, left_on='current_management_code', right_on='vaccination_org', how='outer')
        .fillna(0)
        .assign(
            prop=lambda x: (x['actual_count'] / (x['actual_count'] + x['expected_count'] + x['invalid_count']) * 100).round(2),
            vaccine_name=vaccine_name
        )
        .drop(['vaccination_org'],axis=1)
    )
    return result

def calculate_ci(k, n, confidence_level=0.95):
    bino = binomtest(k=k, n=n, p=0.05, alternative='two-sided')
    ci = bino.proportion_ci(confidence_level=confidence_level)
    return pd.Series([ci.low * 100, ci.high * 100], index=['lower_ci', 'upper_ci'])