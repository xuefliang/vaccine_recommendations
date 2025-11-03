import polars as pl
from recommended_date import *
from data_process import *
from cohort_vaccination_coverage import *
from period_vaccination_coverage import *

# 使用示例
if __name__ == "__main__":
    vaccine_tbl = pl.read_excel("ym_bm.xlsx")

    # 截止日期
    cutoff_date = "2021-06-30"

    person = load_and_process_person_data(
        file_path="/mnt/d/标准库接种率/data/person2.csv",
        cutoff_date=cutoff_date,
        vaccine_tbl=vaccine_tbl,
    )

    # 数据验证
    validate_person_data(person)

    recommendations = get_vaccine_recommendations(person)

    # 时段接种率
    period_coverage = period_vaccination_coverage(person, recommendations)

    # 队列接种率
    cohort_coverage = cohort_vaccination_coverage(person)

    # 查看特定接种单位的数据
    tmp = period_coverage.filter(pl.col("接种单位") == "333647265032")

    tmp = cohort_coverage.filter(pl.col("current_management_code") == "333647265032")

    # 获取特定疫苗推荐
    # hbv_recommendations = get_recommendations_by_vaccine(person, "乙肝疫苗")
    # 获取特定人员推荐
    # person_recommendations = get_recommendations_by_person(person, "0e09122b2d9e4e2e9726faa0eb65d639")
    # 获取逾期推荐
    # overdue = get_overdue_recommendations(person)
