import polars as pl


# --------------------------------------------------
# 配置表
# --------------------------------------------------
VACCINE_RULES = [
    # 卡介苗第1针
    {
        "vaccine_name": "卡介苗",
        "vacc_code": "0101",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "1d",
        "exclude_condition": lambda t: (t["vaccine_name"] == "卡介苗") | (t["age"] > 4),
    },
    # 乙肝疫苗第1针
    {
        "vaccine_name": "乙肝疫苗",
        "vacc_code": "0201",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "1d",
        "exclude_condition": lambda t: (t["vaccine_name"] == "乙肝疫苗") & (t["age_month"] < 1),
    },
    # 乙肝疫苗第2针
    {
        "vaccine_name": "乙肝疫苗",
        "vacc_code": "0201",
        "seq": 2,
        "calculation_logic": "hbv_2",
    },
    # 乙肝疫苗第3针
    {
        "vaccine_name": "乙肝疫苗",
        "vacc_code": "0201",
        "seq": 3,
        "calculation_logic": "hbv_3",
    },
    # 乙肝疫苗第4针
    {
        "vaccine_name": "乙肝疫苗",
        "vacc_code": "0201",
        "seq": 4,
        "calculation_logic": "hbv_4",
    },
    # 脊灰疫苗第1针
    {
        "vaccine_name": "脊灰疫苗",
        "vacc_code": "0303",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "2mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "脊灰疫苗")
        & ((t["age_month"] < 2) | (t["age"] > 6)),
    },
    # 脊灰疫苗第2针
    {
        "vaccine_name": "脊灰疫苗",
        "vacc_code": "0303",
        "seq": 2,
        "based_on": {"vaccine": "脊灰疫苗", "seq": 1},
        "fallback_date": "birth_date",
        "fallback_offset": "3mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "脊灰疫苗")
        & ((t["age_month"] < 3) | (t["age"] > 6)),
    },
    # 脊灰疫苗第3针
    {
        "vaccine_name": "脊灰疫苗",
        "vacc_code": "0311",
        "seq": 3,
        "based_on": {"vaccine": "脊灰疫苗", "seq": 2},
        "fallback_date": "birth_date",
        "fallback_offset": "4mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "脊灰疫苗")
        & ((t["age_month"] < 4) | (t["age"] > 6)),
    },
    # 脊灰疫苗第4针
    {
        "vaccine_name": "脊灰疫苗",
        "vacc_code": "0311",
        "seq": 4,
        "based_on": {"vaccine": "脊灰疫苗", "seq": 3},
        "fallback_date": "birth_date",
        "fallback_offset": "4y",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "脊灰疫苗")
        & ((t["age"] < 4) | (t["age"] > 6)),
    },
    # 百白破疫苗第1针
    {
        "vaccine_name": "百白破疫苗",
        "vacc_code": "0402",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "2mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "百白破疫苗")
        & ((t["age_month"] < 2) | (t["age"] > 7)),
    },
    # 百白破疫苗第2针
    {
        "vaccine_name": "百白破疫苗",
        "vacc_code": "0402",
        "seq": 2,
        "based_on": {"vaccine": "百白破疫苗", "seq": 1},
        "fallback_date": "birth_date",
        "fallback_offset": "4mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "百白破疫苗")
        & ((t["age_month"] < 4) | (t["age"] > 7)),
    },
    # 百白破疫苗第3针
    {
        "vaccine_name": "百白破疫苗",
        "vacc_code": "0402",
        "seq": 3,
        "based_on": {"vaccine": "百白破疫苗", "seq": 2},
        "fallback_date": "birth_date",
        "fallback_offset": "6mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "百白破疫苗")
        & ((t["age_month"] < 6) | (t["age"] > 7)),
    },
    # 百白破疫苗第4针
    {
        "vaccine_name": "百白破疫苗",
        "vacc_code": "0402",
        "seq": 4,
        "based_on": {"vaccine": "百白破疫苗", "seq": 3},
        "fallback_date": "birth_date",
        "fallback_offset": "18mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "百白破疫苗")
        & ((t["age_month"] < 18) | (t["age"] > 7)),
    },
    # 百白破疫苗第5针
    {
        "vaccine_name": "百白破疫苗",
        "vacc_code": "0402",
        "seq": 5,
        "based_on": {"vaccine": "百白破疫苗", "seq": 4},
        "fallback_date": "birth_date",
        "fallback_offset": "6y",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "百白破疫苗")
        & ((t["age"] < 6) | (t["age"] > 7)),
    },
    # 白破疫苗第1针
    {
        "vaccine_name": "白破疫苗",
        "vacc_code": "0601",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "7y",
        "exclude_condition": lambda t: (t["vaccine_name"] == "白破疫苗")
        & ((t["age"] < 7) | (t["age"] > 11)),
    },
    # 含麻疹成分疫苗第1针
    {
        "vaccine_name": "含麻疹成分疫苗",
        "vacc_code": "1201",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "8mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "含麻疹成分疫苗")
        & ((t["age_month"] < 8) | (t["age"] > 6)),
    },
    # 含麻疹成分疫苗第2针
    {
        "vaccine_name": "含麻疹成分疫苗",
        "vacc_code": "1201",
        "seq": 2,
        "based_on": {"vaccine": "含麻疹成分疫苗", "seq": 1},
        "fallback_date": "birth_date",
        "fallback_offset": "18mo",
        "interval": "1mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "含麻疹成分疫苗")
        & ((t["age_month"] < 18) | (t["age"] > 7)),
    },
    # A群流脑疫苗第1针
    {
        "vaccine_name": "A群流脑疫苗",
        "vacc_code": "1601",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "6mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "A群流脑疫苗")
        & ((t["age_month"] < 6) | (t["age_month"] > 23)),
    },
    # A群流脑疫苗第2针
    {
        "vaccine_name": "A群流脑疫苗",
        "vacc_code": "1601",
        "seq": 2,
        "calculation_logic": "mav_2",
    },
    # A群C群流脑疫苗第1针
    {
        "vaccine_name": "A群C群流脑疫苗",
        "vacc_code": "1701",
        "seq": 1,
        "calculation_logic": "mac_1",
    },
    # A群C群流脑疫苗第2针
    {
        "vaccine_name": "A群C群流脑疫苗",
        "vacc_code": "1701",
        "seq": 2,
        "calculation_logic": "mac_2",
    },
    # 乙脑疫苗第1针
    {
        "vaccine_name": "乙脑疫苗",
        "vacc_code": "1801",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "8mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "乙脑疫苗")
        & ((t["age_month"] < 8) | (t["age"] > 6)),
    },
    # 乙脑疫苗第2针
    {
        "vaccine_name": "乙脑疫苗",
        "vacc_code": "1801",
        "seq": 2,
        "based_on": {"vaccine": "乙脑疫苗", "seq": 1},
        "fallback_date": "birth_date",
        "fallback_offset": "2y",
        "interval": "12mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "乙脑疫苗")
        & ((t["age"] < 2) | (t["age"] > 6)),
    },
    # 甲肝疫苗第1针
    {
        "vaccine_name": "甲肝疫苗",
        "vacc_code": "1901",
        "seq": 1,
        "base_date": "birth_date",
        "offset": "18mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "甲肝疫苗")
        & ((t["age_month"] < 18) | (t["age"] > 6)),
    },
    # 甲肝疫苗第2针
    {
        "vaccine_name": "甲肝疫苗",
        "vacc_code": "1903",
        "seq": 2,
        "based_on": {"vaccine": "甲肝疫苗", "seq": 1},
        "fallback_date": "birth_date",
        "fallback_offset": "2y",
        "interval": "6mo",
        "exclude_condition": lambda t: (t["vaccine_name"] == "甲肝疫苗")
        & ((t["age_month"] < 24) | (t["age"] > 6)),
    },
]

# --------------------------------------------------
# 通用聚合
# --------------------------------------------------
def _agg_person(df: pl.DataFrame) -> pl.DataFrame:
    """与接种率2.py完全一致的聚合列"""
    return df.group_by("id_x").agg(
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first(),
    )

# --------------------------------------------------
# 三种计算模式
# --------------------------------------------------
def _from_birth(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """与接种率2.py _calculate_from_birth 完全一致"""
    out = df.with_columns(
        pl.col(rule["base_date"]).dt.offset_by(rule["offset"]).alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    )
    if "exclude_condition" in rule:
        exclude = rule["exclude_condition"](out)
        out = out.with_columns(
            pl.when(exclude).then(None).otherwise(pl.col("recommended_dates")).alias("recommended_dates")
        )
    return _agg_person(out)

def _from_previous(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """与接种率2.py _calculate_based_on_previous 完全一致"""
    based_vaccine, based_seq = rule["based_on"]["vaccine"], rule["based_on"]["seq"]
    fb_date, fb_offset, interval = rule["fallback_date"], rule["fallback_offset"], rule["interval"]

    out = df.with_columns(
        pl.when(
            (pl.col("vaccination_seq") == based_seq) & (pl.col("vaccine_name") == based_vaccine)
        )
        .then(
            pl.max_horizontal(
                pl.col(fb_date).dt.offset_by(fb_offset),
                pl.col("vaccination_date").dt.offset_by(interval),
            )
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    )
    if "exclude_condition" in rule:
        exclude = rule["exclude_condition"](out)
        out = out.with_columns(
            pl.when(exclude).then(None).otherwise(pl.col("recommended_dates")).alias("recommended_dates")
        )
    return _agg_person(out)

# --------------------------------------------------
# 特殊逻辑——与接种率2.py完全一致
# --------------------------------------------------
def _hbv_2(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == "乙肝疫苗")
        )
        .then(
            pl.max_horizontal(
                pl.col("birth_date").dt.offset_by("1mo"),
                pl.col("vaccination_date").dt.offset_by("1mo"),
            )
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    )
    return _agg_person(out)

def _hbv_3(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    tmp = df.with_columns(
        has_dose2=(
            ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == "乙肝疫苗"))
            .any()
            .over("id_x")
        )
    ).with_columns(
        from_dose1=pl.when(
            (pl.col("vaccination_seq") == 1)
            & (pl.col("vaccine_name") == "乙肝疫苗")
            & (pl.col("age") < 1)
            & (pl.col("has_dose2"))
        )
        .then(pl.col("vaccination_date").dt.offset_by("6mo"))
        .when(
            (pl.col("vaccination_seq") == 1)
            & (pl.col("vaccine_name") == "乙肝疫苗")
            & (pl.col("age") >= 1)
            & (pl.col("has_dose2"))
        )
        .then(pl.col("vaccination_date").dt.offset_by("4mo"))
        .otherwise(None),
        from_dose2=pl.when(
            (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == "乙肝疫苗") & (pl.col("age") < 1)
        )
        .then(pl.col("vaccination_date").dt.offset_by("1mo"))
        .when(
            (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == "乙肝疫苗") & (pl.col("age") >= 1)
        )
        .then(pl.col("vaccination_date").dt.offset_by("60d"))
        .otherwise(None),
        recommended_vacc=pl.lit(rule["vacc_code"]),
        recommended_seq=pl.lit(rule["seq"]),
    )
    out = tmp.with_columns(
        recommended_dates=pl.max_horizontal("from_dose1", "from_dose2")
    )
    out = out.with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccine_name") == "乙肝疫苗") & ((pl.col("age_month") < 6) | (pl.col("age_month") > 6))
        )
        .then(None)
        .otherwise(pl.col("recommended_dates"))
    )
    return _agg_person(out)


def _hbv_4(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            (
                (pl.col("hepatitis_mothers") == "1")
                | ((pl.col("hepatitis_mothers") == "3") & (pl.col("birth_weight") < 2000))
            )
            & (pl.col("vaccination_seq") == 3)
            & (pl.col("vaccine_name") == "乙肝疫苗")
        )
        .then(pl.col("vaccination_date").dt.offset_by("5mo"))
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    )
    return _agg_person(out)

def _mav_2(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == "A群流脑疫苗"))
            | (
                (pl.col("vaccination_seq") == 1)
                & (pl.col("vaccination_code").is_in(["1702", "1703", "1704"]))
            )
        )
        .then(
            pl.max_horizontal(
                pl.col("birth_date").dt.offset_by("9mo"),
                pl.col("vaccination_date").dt.offset_by("3mo"),
            )
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    ).with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccine_name") == "A群流脑疫苗")
            & ((pl.col("age_month") < 9) | (pl.col("age_month") > 23))
        )
        .then(None)
        .otherwise(pl.col("recommended_dates"))
    )
    return _agg_person(out)

def _mac_1(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    tmp = df.with_columns(
        his_ma=(
            ((pl.col("vaccine_name") == "A群流脑疫苗") & (pl.col("age") >= 2))
            .sum()
            .over("id_x")
        )
    ).with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == "A群流脑疫苗")
        )
        .then(pl.col("birth_date").dt.offset_by("3y"))
        .otherwise(None),
        recommended_vacc=pl.lit(rule["vacc_code"]),
        recommended_seq=pl.lit(rule["seq"]),
    )
    tmp = tmp.with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccine_name") == "A群C群流脑疫苗") & ((pl.col("age") < 3) | (pl.col("age") > 6))
        )
        .then(None)
        .otherwise(pl.col("recommended_dates"))
    )
    tmp = tmp.with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccination_seq") == 1)
            & (pl.col("vaccine_name") == "A群流脑疫苗")
            & (pl.col("his_ma") == 1)
        )
        .then(
            pl.max_horizontal(
                pl.col("birth_date").dt.offset_by("3y"),
                pl.col("vaccination_date").dt.offset_by("3mo"),
            )
        )
        .otherwise(pl.col("recommended_dates"))
    )
    out = tmp.with_columns(
        recommended_dates=pl.when(pl.col("his_ma") == 0)
        .then(pl.col("birth_date").dt.offset_by("3y"))
        .otherwise(pl.col("recommended_dates"))
    )
    return _agg_person(out)

def _mac_2(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == "A群C群流脑疫苗"))
            | (
                (pl.col("vaccination_seq") == 1)
                & (pl.col("vacc_month") >= 24)
                & (pl.col("vaccination_code").is_in(["1702", "1703", "1704"]))
            )
        )
        .then(
            pl.max_horizontal(
                pl.col("birth_date").dt.offset_by("5y"),
                pl.col("vaccination_date").dt.offset_by("3y"),
            )
        )
        .otherwise(None)
        .alias("recommended_dates"),
        pl.lit(rule["vacc_code"]).alias("recommended_vacc"),
        pl.lit(rule["seq"]).alias("recommended_seq"),
    ).with_columns(
        recommended_dates=pl.when(
            (pl.col("vaccine_name") == "A群C群流脑疫苗") & (pl.col("age") < 5)
        )
        .then(None)
        .otherwise(pl.col("recommended_dates"))
    )
    return _agg_person(out)

# --------------------------------------------------
# 主入口
# --------------------------------------------------
def calculate_all_vaccine_recommendations(person_data: pl.DataFrame) -> pl.DataFrame:
    """
    完全复用接种率2.py逻辑，仅做结构重构：
    1. 规则表独立
    2. 通用计算函数拆分
    3. 聚合逻辑统一
    """
    results = []
    for rule in VACCINE_RULES:
        try:
            if "calculation_logic" in rule:
                logic = rule["calculation_logic"]
                if logic == "hbv_2":
                    res = _hbv_2(person_data, rule)
                elif logic == "hbv_3":
                    res = _hbv_3(person_data, rule)
                elif logic == "hbv_4":
                    res = _hbv_4(person_data, rule)
                elif logic == "mav_2":
                    res = _mav_2(person_data, rule)
                elif logic == "mac_1":
                    res = _mac_1(person_data, rule)
                elif logic == "mac_2":
                    res = _mac_2(person_data, rule)
                else:
                    continue
            elif "based_on" in rule:
                res = _from_previous(person_data, rule)
            else:
                res = _from_birth(person_data, rule)

            if res is not None and not res.is_empty():
                results.append(res)
        except Exception as e:
            print(f"计算疫苗 {rule['vacc_code']} 第 {rule['seq']} 针时出错: {e}")
            continue

    if not results:
        return pl.DataFrame()

    final = pl.concat(results)
    return final.sort(["id_x", "recommended_vacc", "recommended_seq"])

# --------------------------------------------------
# 使用示例
# --------------------------------------------------
if __name__ == "__main__":
    vaccine_tbl = pl.read_excel("ym_bm.xlsx")

person=(
    pl.read_csv('/mnt/d/标准库接种率/data/person2.csv',schema_overrides={"vaccination_code":pl.Utf8,"birth_date":pl.Datetime,"vaccination_date":pl.Datetime,"hepatitis_mothers":pl.Utf8})
    .with_columns([
        pl.lit('2021-01-15').str.to_date().dt.month_end().alias('expiration_date')
    ])
    .with_columns([
        (
            pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year() -
            pl.when(
                (pl.col("expiration_date").dt.month() < pl.col("birth_date").dt.month()) |
                (
                    (pl.col("expiration_date").dt.month() == pl.col("birth_date").dt.month()) &
                    (pl.col("expiration_date").dt.day() < pl.col("birth_date").dt.day())
                )
            ).then(1).otherwise(0)
        ).alias('age')])
    .with_columns([
    (
        (pl.col("vaccination_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
        (pl.col("vaccination_date").dt.month() - pl.col("birth_date").dt.month()) + pl.when(pl.col("vaccination_date").dt.day() >= pl.col("birth_date").dt.day())
        .then(0)
        .otherwise(-1)
    ).alias('vacc_month')])
    .with_columns([(
        (pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
        (pl.col("expiration_date").dt.month() - pl.col("birth_date").dt.month()) + pl.when(pl.col("expiration_date").dt.day() >= pl.col("birth_date").dt.day())
        .then(0)
        .otherwise(-1)
    ).alias('age_month')])
    .join(vaccine_tbl,left_on="vaccination_code",right_on="小类编码",how='left')
    .with_columns(pl.col("vaccine_name").str.split(","))
    .explode("vaccine_name")
    .filter(pl.col("vaccination_date")<=pl.col("expiration_date"))
    .sort(["id_x", "vaccine_name", "vaccination_date"])
    .with_columns([
        pl.int_range(pl.len()).add(1).alias('vaccination_seq').over(["id_x", "vaccine_name"])
    ])
)


