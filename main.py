import polars as pl
from 推荐时间 import *
from 数据处理 import *
from 出生队列接种率 import *
from 时段接种率 import *

# 使用示例
if __name__ == "__main__":
    vaccine_tbl=pl.read_excel('ym_bm.xlsx')

    cutoff_date='2021-12-31'

    person = load_and_process_person_data(
        file_path='/mnt/d/标准库接种率/data/person2.csv',
        cutoff_date=cutoff_date,
        vaccine_tbl=vaccine_tbl
    )

    recommendations = get_consolidated_vaccine_recommendations(person).with_columns(
        pl.lit(cutoff_date).str.to_date().dt.month_start().alias("mon_start"),
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias("mon_end"),
    )

    #时段接种率
    all_vaccine_coverage = period_vaccination_coverage(person, recommendations)

    #队列接种率
    result = cohort_vaccination_coverage(person)
    
    # 查看特定接种单位的数据
    tmp = (
        all_vaccine_coverage
        .filter(pl.col('接种单位') == 333647265032)
    )

    tmp=(
        result
        .filter(pl.col('current_management_code')==333647265032)
    )
