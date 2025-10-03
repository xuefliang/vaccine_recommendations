from datetime import datetime, timedelta, date
from typing import List
import ibis
from ibis.expr import datatypes
from ibis import _
import ibis.selectors as s
import pandas as pd
import re
ibis.options.interactive = True


def unite(columns: list, separator: str = '_'):
    """
    Create a concatenated expression from multiple columns with a specified separator.
    
    Args:
    columns (list): List of column names to concatenate.
    separator (str): The separator to use between columns. Default is '_'.
    
    Returns:
    An ibis expression representing the concatenated columns.
    """
    if not columns:
        raise ValueError("The columns list cannot be empty.")

    # Start with the first column
    concat_expr = _[columns[0]].fillna('')

    # Add the rest of the columns with separators
    for col in columns[1:]:
        concat_expr = concat_expr.concat(separator, _[col].fillna(''))

    return concat_expr

def split(column: str, separator: str = '_', num_parts: int = None):
    """
    Split a column into multiple parts based on a separator.

    Args:
    column (str): The name of the column to split.
    separator (str): The separator to use for splitting. Default is '_'.
    num_parts (int, optional): The number of parts to split into. If None, splits into all possible parts.

    Returns:
    A dictionary of ibis expressions, each representing a part of the split column.
    """
    split_expr = _[column].split(separator)
    
    if num_parts is None:
        # If num_parts is not specified, we'll create as many parts as possible
        # We'll use a large number (e.g., 100) and then filter out the None values later
        num_parts = 100

    result = {}
    for i in range(num_parts):
        part_name = f"{column}_part_{i+1}"
        part_expr = split_expr[i]
        result[part_name] = part_expr

    return result


def load_excel(file_path,skip=0):
    """
    读取Excel文件并将其转换为Ibis内存表
    
    参数:
    file_path (str): Excel文件的路径
    
    返回:
    ibis.memtable: 包含Excel数据的Ibis内存表
    """
    # 读取Excel文件
    df = pd.read_excel(file_path,skiprows=skip)
    
    # 将DataFrame转换为Ibis内存表
    ibis_table = ibis.memtable(df)
    
    return ibis_table

def clean_names(x):
        x = x.lower()
        x = re.sub(r'[\(\),]', '_', x)
        x = re.sub(r'_+', '_', x).strip('_')
        x = re.sub(r'\.', '_', x)
        return x

# con=ibis.duckdb.connect()

rk=(
    load_excel("/mnt/c/Users/xuefliang/Desktop/新冠/新冠入库tj.xlsx")
    .select('sccj_mc','ym_ph')
    .distinct()
)

jz = (
    ibis.read_csv("/mnt/c/Users/xuefliang/Downloads/新冠存疑.csv")
    .rename(clean_names)
    .asof_join(rk,on='ym_ph')
    .drop('ym_ph_right')
    .order_by('substr_jzdd_dm_1_6','ym_ph')
)


jz.to_pandas().to_excel("/mnt/c/Users/xuefliang/Downloads/新冠存疑.xlsx")


rk_1=(
    load_excel("/mnt/c/Users/xuefliang/Downloads/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_1735111095759/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_1735111095759.xlsx",skip=3)
)

rk_2=(
    load_excel("/mnt/c/Users/xuefliang/Downloads/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_1735111095759/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_17351110957591.xlsx",skip=3)
)

rk_3=(
    load_excel("/mnt/c/Users/xuefliang/Downloads/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_1735111095759/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_17351110957592.xlsx",skip=3)
)

rk_4=(
    load_excel("/mnt/c/Users/xuefliang/Downloads/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_1735111095759/入库信息查询_甘肃省_开始2024-01-01_结束2024-12-25_17351110957593.xlsx",skip=3)
)

rk = (
    rk_1.union(rk_2).union(rk_3).union(rk_4)
    .mutate(入库时间=_.入库时间.cast('timestamp'))
    .filter((_.入库时间 >= ibis.literal('2024-12-01 00:00:00').cast('timestamp')) &
            (_.入库时间 <= ibis.literal('2024-12-24 23:59:59').cast('timestamp')))
    .order_by('入库时间')
)

sheng=(
    load_excel("/mnt/c/Users/xuefliang/Downloads/省级12.xlsx")
    .rename(clean_names)
)

####省有国家无
result=(
    sheng.filter(~_.gjsh_rkdh.isin(rk.入库单号)).to_pandas()
)

####省无国家有
result=(
    rk.filter(~_.入库单号.isin(sheng.gjsh_rkdh)).to_pandas()
)

(
    rk.group_by('入库类型')
    .agg(n=_.入库单号.count())
)

(
    sheng.group_by('rklx')
    .agg(n=_.gjsh_rkdh.count())
)

# rk = (
#     rk
#     .mutate(id=(
#         _.入库单号.cast('string').concat(
#             '_', _.疫苗名称.cast('string'),
#             '_', _.生产企业.cast('string'),
#             '_', _.批号.cast('string'),
#             '_', _.入库类型.cast('string'),
#             '_', _.入库数量.cast('string'),
#             '_', _.入库时间.cast('string')
#         )
#     ))
# )

rk = (
    rk
    .mutate(入库数量=_.入库数量.cast('string'))
    .mutate(id=unite(
        columns=['入库单号', '疫苗名称', '生产企业', '批号', '入库类型', '入库数量'],
        separator='_'
    ))
)


# sheng=(
#     sheng
#     .mutate(id=(
#           _.gjsh_rkdh.cast('string').concat(
#             '_', _.ym_mc.cast('string'),
#             '_', _.sccj_mc.cast('string'),
#             '_', _.ym_ph.cast('string'),
#             '_', _.rklx.cast('string'),
#             '_', _.sum_x_rksl.cast('string'),
#             '_',_.xt_rksj.cast('string')
#         )
#     ))
# )

sheng=(
    sheng
    .mutate(sum_x_rksl=_.sum_x_rksl.cast('string'))
    .mutate(id=unite(columns=['gjsh_rkdh', 'ym_mc', 'sccj_mc', 'ym_ph', 'rklx', 'sum_x_rksl','xt_rksj'],separator='_'))
)

yizhi=(
    sheng
    .filter(_.id.isin(rk.id))
    .to_pandas()
)

#入库时间不一致

sheng.count().execute()

test= (
    rk
    .mutate(**split('id', separator='_',num_parts=6))
)