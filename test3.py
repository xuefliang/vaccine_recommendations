import pandas as pd
import ibis

# 读取 .pkl 文件
df = pd.read_pickle("/mnt/d/标准库接种率/data/person2.pkl")
print(ibis.__version__)

df.to_parquet('/mnt/d/标准库接种率/data/person.parquet')

# 创建一个 ibis 连接（这里使用内存数据库作为例子）
con = ibis.duckdb.connect()

# 将 pandas DataFrame 转换为 ibis Table
person = con.read_parquet('/mnt/d/标准库接种率/data/person.parquet')

test2 = (
    test.query("id_x in ['11535fad621f43bb845d99ebefecf94d', '93e9292e85c446df932fdffabb213d15', '58f70daa95a4425bad0f236cd1e0309f', '5907669ae7324da6b5ec298d9e3d3f93', '5ccae9d261624daa83481f49c8d07c71', '68a1515523b64e478c4fc3af32bee0bb', '88c5390bd36d4d7799c8494be1d645cf', '7eeec91a0966477fa2e1df93a6077c9c', '2fa9b08a8dab4f56b6e613db1f46c0d9', '715ab5e592804e55ba26262a669ef750', 'e72a7d86110b4544adecfb81f1f1938e', '51c2dfbe358c4b58a4847e8acda3eb27', '6d6e4b7f7f054d81b4f556902bb00cb7', '95bca6b592c8401f8b16d4bc22f0531c']")
    [['id_x', 'recommended_乙肝疫苗_2']]
)
test2 = test2.drop_duplicates(subset='id_x')