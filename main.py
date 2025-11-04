import polars as pl
from recommended_date import *
from data_process import *
from cohort_vaccination_coverage import *
from period_vaccination_coverage import *

# 使用示例
if __name__ == "__main__":
    vaccine_tbl = pl.read_excel("ym_bm.xlsx")

    person = (
        pl.read_csv(
            "/mnt/c/Users/Administrator/Downloads/标准库接种率+v1.0.9-2024-12-27/标准库数据/person_standard.csv",
        )
        .pipe(lowercase)
        .with_columns(
            pl.col("birth_weight").replace("", None)
        )
        .cast({
            "id": pl.String,
            "birth_date": pl.String,
            "hepatitis_mothers": pl.String,
            "current_management_code": pl.String,
            "birth_weight": pl.Float64, 
        })
    )

    vaccination = (
        pl.read_csv(
            "/mnt/c/Users/Administrator/Downloads/标准库接种率+v1.0.9-2024-12-27/标准库数据/person_standard_vaccination.csv",
        )
        .pipe(lowercase)
        .cast(
            {
                "id": pl.String,
                "person_id": pl.String,
                "type_vaccination_code": pl.Int64,
                "vaccination_code": pl.String,
                "vaccination_seq": pl.Int64,
                "vaccination_date": pl.String,
                "vaccination_site_code": pl.String,
                "batch_number": pl.String,
                "vaccination_org": pl.String,
                "entry_org": pl.String,
                "entry_date": pl.String,
                "temperaturetest_mode_code": pl.Int64,
                "vac_buying_price": pl.Float64,
                "manufacturer_code": pl.String,
            }
        )
    )

    # 合并数据
    person_vacc = vaccination.join(
        person, left_on="person_id", right_on="id", how="left"
    ).with_columns(
        pl.col("birth_date").str.to_datetime(format="%Y%m%dT%H%M%S"),
        pl.col("vaccination_date").str.to_datetime(format="%Y%m%dT%H%M%S"),
    )

    # 查看合并后的数据类型
    print(person_vacc.schema)

    # 合并数据
    person_vacc = process_person_data(
        person=person_vacc, cutoff_date="2021-12-27", vaccine_tbl=vaccine_tbl
    )

    # 数据验证
    validate_person_data(person_vacc)

    recommendations = get_vaccine_recommendations(person_vacc)

    # 时段接种率
    period_coverage = period_vaccination_coverage(person_vacc, recommendations)

    # 队列接种率
    cohort_coverage = cohort_vaccination_coverage(person_vacc)

    # 查看特定接种单位的数据
    tmp = period_coverage.filter(pl.col("接种单位") == "333647265032")

    tmp = cohort_coverage.filter(pl.col("current_management_code") == "333647265032")

    # 获取特定疫苗推荐
    # hbv_recommendations = get_recommendations_by_vaccine(person, "乙肝疫苗")
    # 获取特定人员推荐
    # person_recommendations = get_recommendations_by_person(person, "0e09122b2d9e4e2e9726faa0eb65d639")
    # 获取逾期推荐
    # overdue = get_overdue_recommendations(person)
