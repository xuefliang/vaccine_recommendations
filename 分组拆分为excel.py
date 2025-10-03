import pandas as pd 
import os

df=pd.read_excel('/mnt/c/Users/xuefliang/Desktop/新冠/市级统计.xlsx')

output_dir = r'/mnt/c/Users/xuefliang/Desktop/新冠'

(
    df.groupby('市州') # 分组
    # 保存导出 Excel
    .apply(lambda d: d.to_excel(os.path.join(output_dir, f'{d.name}.xlsx'), index=False))
)