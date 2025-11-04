import polars as pl


def cohort_vaccination_coverage(person, management_code=None, age=None):
    """
    计算出生队列接种率

    Parameters:
    -----------
    person : polars.DataFrame
        包含疫苗接种信息的数据框
    management_code : int, optional
        管理单位代码，默认为None（不筛选）
    age : int, optional
        年龄筛选条件，默认为None（不筛选）

    Returns:
    --------
    polars.DataFrame
        包含接种率的结果数据框
    """
    # 预过滤数据
    filtered_person = person.filter((pl.col("age") > 0) & (pl.col("age") < 18))

    # 根据management_code筛选
    if management_code is not None:
        filtered_person = filtered_person.filter(
            pl.col("current_management_code") == management_code
        )

    # 计算接种率
    result = (
        filtered_person.group_by(["current_management_code", "age"])
        .agg(pl.col("person_id").n_unique().alias("应种数"))
        .join(
            filtered_person.group_by(
                ["current_management_code", "age", "vaccine_name", "vaccination_seq"]
            ).agg(pl.col("person_id").n_unique().alias("接种数")),
            on=["current_management_code", "age"],
            how="left",
        )
        .with_columns(
            (pl.col("接种数") / pl.col("应种数") * 100).round(2).alias("接种率")
        )
        .sort(["current_management_code", "age", "vaccine_name"])
    )

    # 根据age筛选
    if age is not None:
        result = result.filter(pl.col("age") == age)

    return result
