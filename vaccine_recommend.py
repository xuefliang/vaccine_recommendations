import polars as pl
from datetime import datetime
from typing import Any, Dict, Optional

# ----------------------------------------------------------------------
# 特殊计算处理函数
# ----------------------------------------------------------------------


def _apply_hbv_dose3_rules(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """
    乙肝疫苗第3剂推荐时间，完全复刻接种率.py逻辑
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
    A群C群流脑疫苗第1剂推荐时间，完全复刻接种率.py逻辑
    """
    df = (
        df.with_columns(
            ((pl.col("vaccine_name") == "A群流脑疫苗") & (pl.col("age") >= 2))
            .sum()
            .over("person_id")
            .alias("his_ma")
        )
        .filter(
            ((pl.col("his_ma") == 2) & (pl.col("vaccine_name") == "A群流脑疫苗"))
            | ((pl.col("his_ma") == 1) & (pl.col("vaccine_name") == "A群流脑疫苗"))
            | (pl.col("his_ma") == 0)
        )
        .with_columns(
            pl.when(pl.col("his_ma") == 2)
            .then(pl.col("birth_date").dt.offset_by("3y"))
            .when(pl.col("his_ma") == 1)
            .then(
                pl.max_horizontal(
                    [
                        pl.col("birth_date").dt.offset_by("2y"),
                        pl.col("vaccination_date").dt.offset_by("3mo"),
                    ]
                )
            )
            .otherwise(pl.col("birth_date").dt.offset_by("2y"))
            .alias("recommended_dates")
        )
    )

    return df


SPECIAL_CALC_HANDLERS = {
    "hbv_dose3": _apply_hbv_dose3_rules,
    "mac_dose1": _apply_mac_dose1_rules,
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
            "dependency": {"prev_dose": 3, "min_interval": "1mo"},
            "special_calc": None,
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 5,
            "base_schedule": "6y",
            "dependency": {"prev_dose": 4, "min_interval": "1mo"},
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
            "special_calc": None,
        },
        {
            "vaccine_name": "A群流脑疫苗",
            "vaccine_category": "A群流脑疫苗",
            "dose": 2,
            "base_schedule": "9mo",
            "dependency": {
                "prev_dose": 1,
                "min_interval": "3mo",
                "prev_vaccine": "A群流脑疫苗",
            },
            "special_calc": None,
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
            "dependency": {"prev_dose": 1, "min_interval": "3y"},
            "special_calc": None,
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
    status_override = config.get("status_override")

    df = person.clone()

    if special_calc in SPECIAL_CALC_HANDLERS:
        df = SPECIAL_CALC_HANDLERS[special_calc](df, config)
    else:
        recommended_expr = _build_recommended_date_expr(
            base_schedule, dependency, special_calc, vaccine_name
        )
        df = df.with_columns(recommended_expr.alias("recommended_dates"))

    df = df.with_columns(
        [
            pl.lit(vaccine_name).alias("recommended_vacc"),
            pl.lit(dose).alias("recommended_seq"),
        ]
    )

    status_expr = _get_vaccination_status_check(vaccine_category, dose)
    df = df.with_columns(status_expr.alias("recommended_dates"))

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


def _get_vaccination_status_check(
    vaccine_category: str, dose: int
) -> pl.Expr:
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


def get_consolidated_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
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
    return get_consolidated_vaccine_recommendations(person).filter(
        pl.col("recommended_vacc") == vaccine_name
    )


def get_recommendations_by_person(person: pl.DataFrame, person_id: str) -> pl.DataFrame:
    """
    获取某个个体的全部推荐结果
    """
    return (
        get_consolidated_vaccine_recommendations(person)
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
        get_consolidated_vaccine_recommendations(person)
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


if __name__ == "__main__":
    validate_person_data(person)

    # 计算所有疫苗推荐时间
    recommendations = get_consolidated_vaccine_recommendations(person).with_columns(
        pl.lit("2021-01-15").str.to_date().dt.month_start().alias("mon_start"),
        pl.lit("2021-01-15").str.to_date().dt.month_end().alias("mon_end"),
    )

    # 获取特定疫苗推荐
    hbv_recommendations = get_recommendations_by_vaccine(person, "乙肝疫苗")
    # 获取特定人员推荐
    person_recommendations = get_recommendations_by_person(person, "0e09122b2d9e4e2e9726faa0eb65d639")
    # 获取超期推荐
    overdue = get_overdue_recommendations(person)
