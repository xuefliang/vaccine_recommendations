import polars as pl
from recommended_date import *
from data_process import *
from cohort_vaccination_coverage import *
from period_vaccination_coverage import *

# 使用示例
if __name__ == "__main__":
    vaccine_tbl = pl.read_excel("ym_bm.xlsx")

    person = pl.read_csv(
        "./doc/标准库数据/person_standard.csv",
        schema_overrides={
            "ID": pl.String,
            "BIRTH_DATE": pl.String,
            "HEPATITIS_MOTHERS": pl.String,
            "CURRENT_MANAGEMENT_CODE": pl.String,
            "BIRTH_WEIGHT": pl.Float64,
            "GENDER_CODE": pl.Int32,
        },
    ).pipe(lowercase)

    vaccination = pl.read_csv(
        "./doc/标准库数据/person_standard_vaccination.csv",
        schema_overrides={
            "ID": pl.String,
            "PERSON_ID": pl.String,
            "TYPE_VACCINATION_CODE": pl.String,
            "VACCINATION_CODE": pl.String,
            "VACCINATION_SEQ": pl.Int32,
            "VACCINATION_DATE": pl.String,
            "VACCINATION_SITE_CODE": pl.String,
            "BATCH_NUMBER": pl.String,
            "VACCINATION_ORG": pl.String,
            "ENTRY_ORG": pl.String,
            "ENTRY_DATE": pl.String,
            "TEMPERATURETEST_MODE_CODE": pl.Int64,
            "VAC_BUYING_PRICE": pl.Float64,
            "MANUFACTURER_CODE": pl.String,
        },
    ).pipe(lowercase)

    # 合并数据
    person_vacc = vaccination.join(
        person, left_on="person_id", right_on="id", how="left"
    ).with_columns(
        pl.col("birth_date").str.to_datetime(format="%Y%m%dT%H%M%S"),
        pl.col("vaccination_date").str.to_datetime(format="%Y%m%dT%H%M%S"),
    )

    # 查看合并后的数据类型
    person_vacc.schema

    # 合并数据
    person_vacc = process_person_data(
        person=person_vacc, cutoff_date="2025-01-05", vaccine_tbl=vaccine_tbl
    )

    # 数据验证
    validate_person_data(person_vacc)

    # 推荐日期
    recommendations = get_vaccine_recommendations(person_vacc)

    # 时段接种率
    period_coverage = period_vaccination_coverage(person_vacc, recommendations)

    # 队列接种率
    cohort_coverage = cohort_vaccination_coverage(person_vacc)

    # 查看特定接种单位的数据
    tmp = period_coverage.filter(pl.col("接种单位") == "498757058369")

    tmp = cohort_coverage.filter(pl.col("current_management_code") == "498757058369")

    # 获取特定疫苗推荐
    # hbv_recommendations = get_recommendations_by_vaccine(person, "乙肝疫苗")
    # 获取特定人员推荐
    # person_recommendations = get_recommendations_by_person(person, "0e09122b2d9e4e2e9726faa0eb65d639")
    # 获取逾期推荐
    # overdue = get_overdue_recommendations(person)
