import polars as pl
from datetime import datetime
from typing import Any, Dict, Optional

# ----------------------------------------------------------------------
# 特殊计算处理函数
# ----------------------------------------------------------------------


def _apply_hbv_dose3_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    乙肝疫苗第3剂推荐时间
    """
    vaccine_name = config["vaccine_name"]

    df = (
        df.with_columns(
            (
                (pl.col("vaccination_seq") == 2)
                & (pl.col("vaccine_name") == vaccine_name)
            )
            .any()
            .over("person_id")
            .alias("has_dose2")
        )
        .with_columns(
            [
                pl.when(
                    (pl.col("vaccination_seq") == 1)
                    & (pl.col("vaccine_name") == vaccine_name)
                    & (pl.col("age") < 1)
                    & pl.col("has_dose2")
                )
                .then(pl.col("vaccination_date").dt.offset_by("6mo"))
                .when(
                    (pl.col("vaccination_seq") == 1)
                    & (pl.col("vaccine_name") == vaccine_name)
                    & (pl.col("age") >= 1)
                    & pl.col("has_dose2")
                )
                .then(pl.col("vaccination_date").dt.offset_by("4mo"))
                .otherwise(None)
                .alias("from_dose1"),
                pl.when(
                    (pl.col("vaccination_seq") == 2)
                    & (pl.col("vaccine_name") == vaccine_name)
                    & (pl.col("age") < 1)
                )
                .then(pl.col("vaccination_date").dt.offset_by("1mo"))
                .when(
                    (pl.col("vaccination_seq") == 2)
                    & (pl.col("vaccine_name") == vaccine_name)
                    & (pl.col("age") >= 1)
                )
                .then(pl.col("vaccination_date").dt.offset_by("60d"))
                .otherwise(None)
                .alias("from_dose2"),
            ]
        )
        .with_columns(
            pl.max_horizontal(
                [
                    pl.col("from_dose1").shift(1).over(["person_id", "vaccine_name"]),
                    pl.col("from_dose2"),
                ]
            ).alias("recommended_dates")
        )
    )

    return df


def _apply_mac_dose1_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    A群C群流脑疫苗第1剂推荐时间
    """
    df = (
        df.with_columns(
            # 为每个人标记A群流脑疫苗免疫剂次数
            ((pl.col("vaccine_name") == "A群流脑疫苗") & (pl.col("age") >= 2))
            .sum()
            .over("person_id")
            .alias("his_ma"),
            # A群C群流脑疫苗 - 24月龄及以后的接种次数
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vacc_month") >= 24)
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .sum()
            .over("person_id")
            .alias("his_mac"),
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vacc_month") < 24)
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .sum()
            .over("person_id")
            .alias("his_mac_before"),
        )
        .filter(  # 合并三个条件
            (
                (pl.col("his_ma") == 2)
                & (pl.col("vaccine_name").is_in(["A群流脑疫苗", "A群C群流脑疫苗"]))
            )
            | (
                (pl.col("his_ma") == 1)
                & (pl.col("vaccine_name").is_in(["A群流脑疫苗", "A群C群流脑疫苗"]))
            )
            | (pl.col("his_ma") == 0)
        )
        .with_columns(
            [
                # 根据his_ma值设置不同的recommended_dates
                pl.when((pl.col("his_ma") == 2) & (pl.col("his_mac") == 0))
                .then(pl.col("birth_date").dt.offset_by("3y"))
                .when((pl.col("his_ma") < 2) & (pl.col("his_mac") == 1))
                .then((pl.col("birth_date").dt.offset_by("3y")))
                .when(
                    (pl.col("his_ma") == 0)
                    & (pl.col("his_mac") == 0)
                    & (pl.col("his_mac_before") == 0)
                )
                .then((pl.col("birth_date").dt.offset_by("2y")))
                .when(
                    (pl.col("his_ma") == 0)
                    & (pl.col("his_mac") == 0)
                    & (pl.col("his_mac_before") >= 2)
                )
                .then((pl.col("birth_date").dt.offset_by("3y")))
                .when(
                    (pl.col("his_ma") == 0)
                    & (pl.col("his_mac") == 0)
                    & (pl.col("his_mac_before") == 1)
                )
                .then((pl.col("birth_date").dt.offset_by("2y")))
                .when(
                    (pl.col("his_ma") == 1)
                    & (pl.col("his_mac") == 0)
                    & (pl.col("his_mac_before") == 0)
                    & (pl.col("vaccine_name") == "A群流脑疫苗")
                )
                .then(
                    pl.max_horizontal(
                        [
                            pl.col("birth_date").dt.offset_by("2y"),
                            pl.col("vaccination_date").dt.offset_by("3mo"),
                        ]
                    )
                )
                .alias("recommended_dates"),
                pl.lit("A群C群流脑疫苗").alias("recommended_vacc"),
                pl.lit(1).alias("recommended_seq"),
            ]
        )
        .with_columns(
            pl.when((pl.col("his_mac") == 1))
            .then(None)
            .otherwise(pl.col("recommended_dates"))
            .alias("recommended_dates")
        )
    )

    return df


def _apply_mac_dose2_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    A群C群流脑疫苗第2剂推荐时间
    """
    df = df.with_columns(
        [
            # 为每个人标记A群流脑疫苗免疫剂次数
            ((pl.col("vaccine_name") == "A群流脑疫苗") & (pl.col("age") >= 2))
            .sum()
            .over("person_id")
            .alias("his_ma"),
            # A群C群流脑疫苗 - 24月龄及以后的接种次数
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vacc_month") >= 24)
                & (pl.col("vacc_month") < 60)
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .sum()
            .over("person_id")
            .alias("his_mac"),
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vacc_month") >= 60)
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .sum()
            .over("person_id")
            .alias("his_mac_5"),
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .sum()
            .over("person_id")
            .alias("ac_max_seq"),
        ]
    ).with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1)
            & (pl.col("vaccine_name") == "A群C群流脑疫苗")
            & (pl.col("his_mac") == 1)
            & (pl.col("his_mac_5") == 0)
        )
        .then(
            pl.max_horizontal(
                [
                    pl.col("birth_date").dt.offset_by("6y"),
                    pl.col("vaccination_date").dt.offset_by("3y"),
                ]
            )
        )
        .when(
            (pl.col("vaccination_seq") == 1)
            & (pl.col("vaccine_name") == "A群C群流脑疫苗")
            & (pl.col("his_mac_5") == 1)
            & (pl.col("his_mac") == 0)
            & (pl.col("vaccination_seq") == pl.col("ac_max_seq"))
        )
        .then(
            pl.max_horizontal(
                [
                    pl.col("birth_date").dt.offset_by("6y"),
                    pl.col("vaccination_date").dt.offset_by("3y"),
                ]
            )
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit("A群C群流脑疫苗").alias("recommended_vacc"),
        pl.lit(2).alias("recommended_seq"),
    )

    return df


def _apply_mav_dose1_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    A群流脑疫苗第1剂推荐时间
    """
    vaccine_name = config["vaccine_name"]

    df = (
        df
        # 标记需要排除的人员（已接种A群C群流脑疫苗第1针且月龄<24）
        .with_columns(
            (
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vaccination_seq") == 1)
                & (pl.col("vacc_month") < 24)
                & (pl.col("vaccination_date") <= pl.col("mon_end"))
            )
            .any()
            .over("person_id")
            .alias("should_exclude")
        )
        # 过滤掉需要排除的人员
        .filter(~pl.col("should_exclude"))
        # 计算推荐日期
        .with_columns(
            [
                pl.col("birth_date").dt.offset_by("6mo").alias("recommended_dates"),
                pl.lit(vaccine_name).alias("recommended_vacc"),
                pl.lit(1).alias("recommended_seq"),
            ]
        )
        # 状态检查
        .with_columns(
            pl.when(
                (pl.col("recommended_seq") == 1)
                & (pl.col("vaccine_name") == vaccine_name)
                & (pl.col("vaccination_seq") == 1)
                & (pl.col("vaccination_date") > pl.col("recommended_dates"))
            )
            .then(pl.col("recommended_dates"))
            .when(~pl.col("vaccine_name").is_in([vaccine_name]))
            .then(pl.col("recommended_dates"))
            .otherwise(None)
            .alias("recommended_dates")
        )
        .drop("should_exclude")
    )

    return df


def _apply_mav_dose2_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    A群流脑疫苗第2剂推荐时间
    包含两部分逻辑：
    1. p1: 基于 A群流脑疫苗第1剂计算（取出生日期+9月 和 接种日期+3月 的较大值）
    2. p2: 基于 A群C群流脑疫苗第1剂计算（根据月龄判断第2针）
    最终合并两部分结果
    """

    # ===== Part 1: A群流脑疫苗 =====
    df_p1 = (
        df.with_columns(
            # 推荐日期：max(出生+9月, 接种+3月)
            recommended_dates=pl.when(
                (pl.col("vaccination_seq") == 1)
                & (pl.col("vaccine_name") == "A群流脑疫苗")
            ).then(
                pl.max_horizontal(
                    [
                        pl.col("birth_date").dt.offset_by("9mo"),
                        pl.col("vaccination_date").dt.offset_by("3mo"),
                    ]
                )
            ),
            recommended_vacc=pl.lit("A群流脑疫苗"),
            recommended_seq=pl.lit(2),
        )
        .with_columns(
            # 判断是否显示推荐
            show_recommendation=(
                (pl.col("vaccine_name") == "A群流脑疫苗")
                & (
                    (pl.col("vaccination_seq") == 1)  # 未种第二针
                    | (  # 或已种但延迟
                        (pl.col("vaccination_seq") == 2)
                        & (pl.col("vaccination_date") > pl.col("recommended_dates"))
                    )
                )
            )
        )
        .filter(pl.col("show_recommendation"))
        .drop("show_recommendation")
    )

    # ===== Part 2: A群C群流脑疫苗 =====
    df_p2 = (
        df.with_columns(
            # 推荐日期：接种日期 + 3月
            recommended_dates=pl.when(
                (pl.col("vaccination_seq") == 1)
                & (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (pl.col("vacc_month") < 24)
            ).then(pl.col("vaccination_date").dt.offset_by("3mo")),
            recommended_vacc=pl.lit("A群流脑疫苗"),
            recommended_seq=pl.lit(2),
        )
        .with_columns(
            # 判断是否显示推荐
            show_recommendation=(
                (pl.col("vaccine_name") == "A群C群流脑疫苗")
                & (
                    (pl.col("vaccination_seq") == 1)  # 未种第二针
                    | (  # 或已种但延迟（根据月龄判断应该是第2针还是第3针）
                        (
                            pl.col("vaccination_seq")
                            == pl.when(pl.col("vacc_month") < 6).then(3).otherwise(2)
                        )
                        & (pl.col("vaccination_date") > pl.col("recommended_dates"))
                    )
                )
            )
        )
        .filter(pl.col("show_recommendation"))
        .drop("show_recommendation")
    )

    # ===== 合并结果 =====
    return pl.concat([df_p1, df_p2])


SPECIAL_CALC_HANDLERS = {
    "hbv_dose3": _apply_hbv_dose3_rules,
    "mac_dose1": _apply_mac_dose1_rules,
    "mac_dose2": _apply_mac_dose2_rules,
    "mav_dose2": _apply_mav_dose2_rules,
    "mav_dose1": _apply_mav_dose1_rules,
}

# ----------------------------------------------------------------------
# 主计算流程
# ----------------------------------------------------------------------


def calculate_all_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
    """
    统一计算所有疫苗的推荐时间
    """
    vaccine_configs = [
        # 卡介苗
        {
            "vaccine_name": "卡介苗",
            "vaccine_category": "卡介苗",
            "dose": 1,
            "base_schedule": "0d",
            "dependency": None,
            "special_calc": None,
        },
        # 乙肝疫苗
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 1,
            "base_schedule": "0d",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 2,
            "base_schedule": "1mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 3,
            "base_schedule": None,
            "dependency": None,
            "special_calc": "hbv_dose3",
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 4,
            "base_schedule": None,
            "dependency": {"prev_dose": 3, "min_interval": "5mo"},
            "special_calc": "high_risk_hbv",
        },
        # 脊灰疫苗
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 1,
            "base_schedule": "2mo",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 2,
            "base_schedule": "3mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 3,
            "base_schedule": "4mo",
            "dependency": {"prev_dose": 2, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 4,
            "base_schedule": "4y",
            "dependency": {"prev_dose": 3, "min_interval": "1mo"},
            "special_calc": None,
        },
        # 百白破疫苗
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 1,
            "base_schedule": "2mo",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 2,
            "base_schedule": "4mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 3,
            "base_schedule": "6mo",
            "dependency": {"prev_dose": 2, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 4,
            "base_schedule": "18mo",
            "dependency": {"prev_dose": 3, "min_interval": "6mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 5,
            "base_schedule": "6y",
            "dependency": {"prev_dose": 4, "min_interval": "12mo"},
            "special_calc": None,
        },
        # 白破疫苗
        {
            "vaccine_name": "白破疫苗",
            "vaccine_category": "白破疫苗",
            "dose": 1,
            "base_schedule": "7y",
            "dependency": None,
            "special_calc": None,
        },
        # 含麻疹成分疫苗
        {
            "vaccine_name": "含麻疹成分疫苗",
            "vaccine_category": "含麻疹成分疫苗",
            "dose": 1,
            "base_schedule": "8mo",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "含麻疹成分疫苗",
            "vaccine_category": "含麻疹成分疫苗",
            "dose": 2,
            "base_schedule": "18mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_calc": None,
        },
        # A群流脑疫苗
        {
            "vaccine_name": "A群流脑疫苗",
            "vaccine_category": "A群流脑疫苗",
            "dose": 1,
            "base_schedule": "6mo",
            "dependency": None,
            "special_calc": "mav_dose1",
        },
        {
            "vaccine_name": "A群流脑疫苗",
            "vaccine_category": "A群流脑疫苗",
            "dose": 2,
            "base_schedule": "9mo",
            "dependency": None,
            "special_calc": "mav_dose2",
        },
        # A群C群流脑疫苗
        {
            "vaccine_name": "A群C群流脑疫苗",
            "vaccine_category": "A群C群流脑疫苗",
            "dose": 1,
            "base_schedule": "2y",
            "dependency": None,
            "special_calc": "mac_dose1",
        },
        {
            "vaccine_name": "A群C群流脑疫苗",
            "vaccine_category": "A群C群流脑疫苗",
            "dose": 2,
            "base_schedule": "6y",
            "dependency": None,
            "special_calc": "mac_dose2",
        },
        # 乙脑疫苗
        {
            "vaccine_name": "乙脑疫苗",
            "vaccine_category": "乙脑疫苗",
            "dose": 1,
            "base_schedule": "8mo",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "乙脑疫苗",
            "vaccine_category": "乙脑疫苗",
            "dose": 2,
            "base_schedule": "2y",
            "dependency": {"prev_dose": 1, "min_interval": "12mo"},
            "special_calc": None,
        },
        # 甲肝疫苗
        {
            "vaccine_name": "甲肝疫苗",
            "vaccine_category": "甲肝疫苗",
            "dose": 1,
            "base_schedule": "18mo",
            "dependency": None,
            "special_calc": None,
        },
        {
            "vaccine_name": "甲肝疫苗",
            "vaccine_category": "甲肝疫苗",
            "dose": 2,
            "base_schedule": "2y",
            "dependency": {"prev_dose": 1, "min_interval": "6mo"},
            "special_calc": "hav_inactivated",
        },
    ]

    recommendations = []
    for config in vaccine_configs:
        try:
            result = _calculate_single_vaccine_recommendation(person, config)
            if result is not None and result.height > 0:
                recommendations.append(result)
        except Exception as exc:
            print(
                f"计算疫苗推荐时间时出错 - {config['vaccine_name']} 第{config['dose']}剂: {exc}"
            )

    return (
        pl.concat(recommendations, how="vertical")
        if recommendations
        else pl.DataFrame()
    )


def _calculate_single_vaccine_recommendation(
    person: pl.DataFrame, config: Dict[str, Any]
) -> Optional[pl.DataFrame]:
    """
    计算单个疫苗推荐时间
    """
    vaccine_name = config["vaccine_name"]
    vaccine_category = config["vaccine_category"]
    dose = config["dose"]
    base_schedule = config.get("base_schedule")
    dependency = config.get("dependency")
    special_calc = config.get("special_calc")

    df = person.clone()

    # 提前创建推荐疫苗信息列
    df = df.with_columns(
        [
            pl.lit(vaccine_name).alias("recommended_vacc"),
            pl.lit(dose).alias("recommended_seq"),
        ]
    )

    # 区分特殊计算和常规计算
    if special_calc in SPECIAL_CALC_HANDLERS:
        # 特殊计算已经包含完整逻辑，不需要状态检查
        df = SPECIAL_CALC_HANDLERS[special_calc](df, config)
    else:
        # 常规计算流程
        recommended_expr = _build_recommended_date_expr(
            base_schedule, dependency, special_calc, vaccine_name
        )
        df = df.with_columns(recommended_expr.alias("recommended_dates"))

        # 应用状态检查
        status_expr = _get_vaccination_status_check(vaccine_category, dose)
        df = df.with_columns(status_expr.alias("recommended_dates"))

    # 聚合结果
    result = df.group_by("person_id").agg(
        [
            pl.col("recommended_dates").drop_nulls().first().alias("recommended_dates"),
            pl.col("recommended_vacc").first().alias("recommended_vacc"),
            pl.col("recommended_seq").first().alias("recommended_seq"),
            pl.col("birth_date").first().alias("birth_date"),
            pl.col("age").first().alias("age"),
            pl.col("age_month").first().alias("age_month"),
            pl.col("entry_org").first().alias("entry_org"),
            pl.col("entry_date").first().alias("entry_date"),
            pl.col("vaccination_org").first().alias("vaccination_org"),
            pl.col("current_management_code").first().alias("current_management_code"),
            pl.col("mon_start").first().alias("mon_start"),
            pl.col("mon_end").first().alias("mon_end"),
        ]
    )

    return result


def _build_recommended_date_expr(
    base_schedule: Optional[str],
    dependency: Optional[Dict[str, Any]],
    special_calc: Optional[str],
    vaccine_name: str,
) -> pl.Expr:
    """
    生成推荐日期表达式
    """
    if special_calc == "high_risk_hbv":
        return (
            pl.when(
                (
                    (pl.col("hepatitis_mothers") == "1")
                    | (
                        (pl.col("hepatitis_mothers") == "3")
                        & (pl.col("birth_weight") < 2000)
                    )
                )
                & (pl.col("vaccination_seq") == 3)
                & (pl.col("vaccine_name") == vaccine_name)
            )
            .then(pl.col("vaccination_date").dt.offset_by("5mo"))
            .otherwise(None)
        )

    if special_calc == "hav_inactivated":
        return (
            pl.when(
                (pl.col("vaccination_seq") == 1)
                & (pl.col("小类名称") == "甲型肝炎灭活疫苗（人二倍体细胞）")
            )
            .then(
                pl.max_horizontal(
                    [
                        pl.col("birth_date").dt.offset_by("2y"),
                        pl.col("vaccination_date").dt.offset_by("6mo"),
                    ]
                )
            )
            .otherwise(None)
        )

    if dependency:
        prev_dose = dependency.get("prev_dose", 1)
        min_interval = dependency.get("min_interval", "0d")
        prev_vaccine = dependency.get("prev_vaccine") or vaccine_name

        max_candidates = []
        if base_schedule:
            max_candidates.append(pl.col("birth_date").dt.offset_by(base_schedule))
        max_candidates.append(pl.col("vaccination_date").dt.offset_by(min_interval))

        return (
            pl.when(
                (pl.col("vaccination_seq") == prev_dose)
                & (pl.col("vaccine_name") == prev_vaccine)
            )
            .then(pl.max_horizontal(max_candidates))
            .otherwise(None)
        )

    if base_schedule:
        return pl.col("birth_date").dt.offset_by(base_schedule)

    return pl.lit(None)


def _get_vaccination_status_check(vaccine_category: str, dose: int) -> pl.Expr:
    """
    生成接种状态校验逻辑
    """
    if dose == 1:
        return (
            pl.when(
                (pl.col("recommended_seq") == dose)
                & (pl.col("vaccine_name") == vaccine_category)
                & (pl.col("vaccination_seq") == dose)
                & (pl.col("vaccination_date") > pl.col("recommended_dates"))
            )
            .then(pl.col("recommended_dates"))
            .when(~pl.col("vaccine_name").is_in([vaccine_category]))
            .then(pl.col("recommended_dates"))
            .otherwise(None)
        )

    return (
        pl.when(
            (pl.col("recommended_seq") == dose)
            & (pl.col("vaccine_name") == vaccine_category)
            & (pl.col("vaccination_seq") == dose)
            & (pl.col("vaccination_date") > pl.col("recommended_dates"))
        )
        .then(pl.col("recommended_dates"))
        .when(
            (pl.col("recommended_seq") == dose)
            & (pl.col("vaccine_name") == vaccine_category)
            & (pl.col("vaccination_seq") == dose - 1)
        )
        .then(pl.col("recommended_dates"))
        .otherwise(None)
    )


# ----------------------------------------------------------------------
# 公共接口
# ----------------------------------------------------------------------


def get_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
    """
    获取全部疫苗推荐结果并合并
    """
    recommendations = calculate_all_vaccine_recommendations(person)
    if recommendations.height == 0:
        return recommendations

    return recommendations.filter(pl.col("recommended_dates").is_not_null()).sort(
        ["person_id", "recommended_dates", "recommended_vacc", "recommended_seq"]
    )


def get_recommendations_by_vaccine(
    person: pl.DataFrame, vaccine_name: str
) -> pl.DataFrame:
    """
    获取特定疫苗的推荐结果
    """
    return get_vaccine_recommendations(person).filter(
        pl.col("recommended_vacc") == vaccine_name
    )


def get_recommendations_by_person(person: pl.DataFrame, person_id: str) -> pl.DataFrame:
    """
    获取某个个体的全部推荐结果
    """
    return (
        get_vaccine_recommendations(person)
        .filter(pl.col("person_id") == person_id)
        .sort("recommended_dates")
    )


def get_overdue_recommendations(
    person: pl.DataFrame, current_date: Optional[str] = None
) -> pl.DataFrame:
    """
    获取超期未种疫苗推荐
    """
    if current_date is None:
        current_date = datetime.now().strftime("%Y-%m-%d")

    current_date_expr = pl.lit(current_date).str.to_date()
    return (
        get_vaccine_recommendations(person)
        .filter(pl.col("recommended_dates") < current_date_expr)
        .sort(["person_id", "recommended_dates"])
    )


def export_recommendations_to_excel(
    recommendations: pl.DataFrame, filename: str
) -> None:
    """
    导出推荐结果至 Excel
    """
    try:
        recommendations.to_pandas().to_excel(filename, index=False)
        print(f"推荐结果已导出到文件: {filename}")
    except Exception as exc:
        print(f"导出文件时出错: {exc}")


def validate_person_data(person: pl.DataFrame) -> bool:
    """
    验证 person 数据是否齐全
    """
    required_fields = [
        "person_id",
        "birth_date",
        "age",
        "age_month",
        "vacc_month",
        "vaccine_name",
        "vaccination_seq",
        "vaccination_date",
        "entry_org",
        "entry_date",
        "vaccination_org",
        "current_management_code",
        "birth_weight",
        "hepatitis_mothers",
    ]
    missing = [field for field in required_fields if field not in person.columns]

    if missing:
        print(f"缺少必要字段: {missing}")
        return False

    print("数据验证通过")
    return True
