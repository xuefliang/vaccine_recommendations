import polars as pl
from recommended_date import *
from data_process import *
from cohort_vaccination_coverage import *
from period_vaccination_coverage import *

# 使用示例
if __name__ == "__main__":
    vaccine_tbl=pl.read_excel('ym_bm.xlsx')

    #截止日期
    cutoff_date='2021-06-30'

    person = load_and_process_person_data(
        file_path='/mnt/d/标准库接种率/data/person2.csv',
        cutoff_date=cutoff_date,
        vaccine_tbl=vaccine_tbl
    )

    recommendations = get_vaccine_recommendations(person).with_columns(
        pl.lit(cutoff_date).str.to_date().dt.month_start().alias("mon_start"),
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias("mon_end"),
    )

    #时段接种率
    period_coverage = period_vaccination_coverage(person, recommendations)

    #队列接种率
    cohort_coverage = cohort_vaccination_coverage(person)
    
    # 查看特定接种单位的数据
    tmp = (
        period_coverage
        .filter(pl.col('接种单位') == '333647265032')
    )

    tmp=(
        cohort_coverage
        .filter(pl.col('current_management_code')=='333647265032')
    )
