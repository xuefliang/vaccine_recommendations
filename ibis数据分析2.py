import ibis
import pandas as pd
import ibis.expr.datatypes as dt
from ibis import _
import ibis.selectors as s
ibis.options.interactive = True

print(ibis.__version__)

def load_excel(file_path):
    """
    读取Excel文件并将其转换为Ibis内存表
    
    参数:
    file_path (str): Excel文件的路径
    
    返回:
    ibis.memtable: 包含Excel数据的Ibis内存表
    """
    # 读取Excel文件
    df = pd.read_excel(file_path)
    
    # 将DataFrame转换为Ibis内存表
    ibis_table = ibis.memtable(df)
    
    return ibis_table

df = load_excel('/mnt/c/Users/xuefliang/Downloads/接种单位列表.xlsx')