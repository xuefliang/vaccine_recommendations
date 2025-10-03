import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend
import pandas as pd
import ibis.expr.datatypes as dt
from ibis import _
import ibis.selectors as s
ibis.options.interactive = True

class CustomDuckDBConnection:
    def __init__(self, backend, connection):
        self.backend = backend
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def read_excel(self, path, sheet_name=0, **kwargs):
        df = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
        table_name = f"excel_data_{ibis.util.guid()}"
        self.connection.con.register(table_name, df)
        return self.connection.table(table_name)

class CustomDuckDBBackend(DuckDBBackend):
    def connect(self, *args, **kwargs):
        connection = super().connect(*args, **kwargs)
        return CustomDuckDBConnection(self, connection)

def custom_duckdb_connect(*args, **kwargs):
    return CustomDuckDBBackend().connect(*args, **kwargs)

# 使用自定义连接函数
con = custom_duckdb_connect(':memory:')

# 读取Excel文件
df = (
    con
    .read_excel('/mnt/c/Users/xuefliang/Downloads/接种单位列表.xlsx')
    .mutate(基层医疗卫生机构接种单位工作类型=ibis.case()
        .when(_.基层医疗卫生机构接种单位工作类型.isnull(), _.管理类型)
        .else_(_.基层医疗卫生机构接种单位工作类型)
        .end())
)

# 执行分组聚合操作
(
    df.group_by('基层医疗卫生机构接种单位工作类型')
    .agg(数量=_.接种单位编码.count())
    .order_by(_.数量.desc())
)

(
    df.filter(_.接种疫苗类型=='仅接种非免疫规划疫苗')
    .group_by(_.接种单位所属地市名称)
    .agg(n=_.接种单位所属区县名称.nunique())
).to_pandas()

(
    df.filter(_.医疗机构许可证有效期截止日期<'2024-10-30')
    .select(_.接种单位所属区县名称,_.接种单位名称,_.医疗机构许可证有效期截止日期)
).to_pandas().to_excel('/mnt/c/Users/xuefliang/Downloads/许可证过期.xlsx')

df=ibis.memtable(pd.read_excel('/mnt/c/Users/xuefliang/Downloads/接种单位列表.xlsx'))