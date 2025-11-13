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
    # 计算每个年龄组的总人数和女性人数
    age_stats = filtered_person.group_by(["current_management_code", "age"]).agg(
        [
            pl.col("person_id").n_unique().alias("全部人数"),
            pl.col("person_id")
            .filter(pl.col("gender_code") == 2)
            .n_unique()
            .alias("女性人数"),
        ]
    )
    # 计算各疫苗的接种数
    vaccination_stats = filtered_person.group_by(
        ["current_management_code", "age", "vaccine_name", "vaccination_seq"]
    ).agg(pl.col("person_id").n_unique().alias("接种数"))
    # 合并数据
    result = (
        vaccination_stats.join(
            age_stats, on=["current_management_code", "age"], how="left"
        )
        .with_columns(
            [
                # 根据是否为HPV疫苗选择不同的分母
                pl.when(
                    pl.col("vaccine_name")=="HPV疫苗"
                )
                .then(pl.col("女性人数"))
                .otherwise(pl.col("全部人数"))
                .alias("应种数"),
            ]
        )
        .with_columns(
            [
                # 计算接种率
                (pl.col("接种数") / pl.col("应种数") * 100).round(2).alias("接种率"),
            ]
        )
        .select(
            [
                "current_management_code",
                "age",
                "vaccine_name",
                "vaccination_seq",
                "应种数",
                "女性人数",
                "接种数",
                "接种率",
            ]
        )
        .filter(pl.col('vaccine_name').is_not_null())
        .sort(["current_management_code", "age", "vaccine_name", "vaccination_seq"])
    )
    # 根据age筛选
    if age is not None:
        result = result.filter(pl.col("age") == age)
    return result
