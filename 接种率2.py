import polars as pl

vaccine_tbl=pl.read_excel('ym_bm.xlsx')

def calculate_all_vaccine_recommendations(person_data:pl.dataframe)->pl.dataframe:
    """
    计算所有疫苗的推荐接种时间
    
    Args:
        person_data: 包含疫苗接种记录的DataFrame
        
    Returns:
        所有疫苗推荐时间的合并DataFrame
    """
    
    # 疫苗推荐规则配置
    vaccine_rules = [
        # 卡介苗第1针
        {
            'vaccine_name': '卡介苗',
            'vacc_code': '0101',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '1d',
            'exclude_condition': "(pl.col('vaccine_name')=='卡介苗') | (pl.col('age') > 4)"
        },
        
        # 乙肝疫苗第1针
        {
            'vaccine_name': '乙肝疫苗',
            'vacc_code': '0201',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '1d',
            'exclude_condition': "(pl.col('vaccine_name')=='乙肝疫苗') & (pl.col('age_month') <1)"
        },
        
        # 乙肝疫苗第2针
        {
            'vaccine_name': '乙肝疫苗',
            'vacc_code': '0201',
            'seq': 2,
            'calculation_logic': 'hbv_2'
        },
        
        # 乙肝疫苗第3针
        {
            'vaccine_name': '乙肝疫苗',
            'vacc_code': '0201',
            'seq': 3,
            'calculation_logic': 'hbv_3'
        },
        
        # 乙肝疫苗第4针
        {
            'vaccine_name': '乙肝疫苗',
            'vacc_code': '0201',
            'seq': 4,
            'calculation_logic': 'hbv_4'
        },
        
        # 脊灰疫苗第1针
        {
            'vaccine_name': '脊灰疫苗',
            'vacc_code': '0303',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '2mo',
            'exclude_condition': "(pl.col('vaccine_name')=='脊灰疫苗') & ((pl.col('age_month') <2) | (pl.col('age') >6))"
        },
        
        # 脊灰疫苗第2针
        {
            'vaccine_name': '脊灰疫苗',
            'vacc_code': '0303',
            'seq': 2,
            'based_on': {'vaccine': '脊灰疫苗', 'seq': 1},
            'fallback_date': 'birth_date',
            'fallback_offset': '3mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='脊灰疫苗') & ((pl.col('age_month') <3) | (pl.col('age') >6))"
        },
        
        # 脊灰疫苗第3针
        {
            'vaccine_name': '脊灰疫苗',
            'vacc_code': '0311',
            'seq': 3,
            'based_on': {'vaccine': '脊灰疫苗', 'seq': 2},
            'fallback_date': 'birth_date',
            'fallback_offset': '4mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='脊灰疫苗') & ((pl.col('age_month') <4) | (pl.col('age') >6))"
        },
        
        # 脊灰疫苗第4针
        {
            'vaccine_name': '脊灰疫苗',
            'vacc_code': '0311',
            'seq': 4,
            'based_on': {'vaccine': '脊灰疫苗', 'seq': 3},
            'fallback_date': 'birth_date',
            'fallback_offset': '4y',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='脊灰疫苗') & ((pl.col('age') <4) | (pl.col('age') >6))"
        },
        
        # 百白破疫苗第1针
        {
            'vaccine_name': '百白破疫苗',
            'vacc_code': '0402',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '2mo',
            'exclude_condition': "(pl.col('vaccine_name')=='百白破疫苗') & ((pl.col('age_month') <2) | (pl.col('age') >7))"
        },
        
        # 百白破疫苗第2针
        {
            'vaccine_name': '百白破疫苗',
            'vacc_code': '0402',
            'seq': 2,
            'based_on': {'vaccine': '百白破疫苗', 'seq': 1},
            'fallback_date': 'birth_date',
            'fallback_offset': '4mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='百白破疫苗') & ((pl.col('age_month') <4) | (pl.col('age') >7))"
        },
        
        # 百白破疫苗第3针
        {
            'vaccine_name': '百白破疫苗',
            'vacc_code': '0402',
            'seq': 3,
            'based_on': {'vaccine': '百白破疫苗', 'seq': 2},
            'fallback_date': 'birth_date',
            'fallback_offset': '6mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='百白破疫苗') & ((pl.col('age_month') <6) | (pl.col('age') >7))"
        },
        
        # 百白破疫苗第4针
        {
            'vaccine_name': '百白破疫苗',
            'vacc_code': '0402',
            'seq': 4,
            'based_on': {'vaccine': '百白破疫苗', 'seq': 3},
            'fallback_date': 'birth_date',
            'fallback_offset': '18mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='百白破疫苗') & ((pl.col('age_month') <18) | (pl.col('age') >7))"
        },
        
        # 百白破疫苗第5针
        {
            'vaccine_name': '百白破疫苗',
            'vacc_code': '0402',
            'seq': 5,
            'based_on': {'vaccine': '百白破疫苗', 'seq': 4},
            'fallback_date': 'birth_date',
            'fallback_offset': '6y',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='百白破疫苗') & ((pl.col('age') <6) | (pl.col('age') >7))"
        },
        
        # 白破疫苗第1针
        {
            'vaccine_name': '白破疫苗',
            'vacc_code': '0601',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '7y',
            'exclude_condition': "(pl.col('vaccine_name')=='白破疫苗') & ((pl.col('age') <7) | (pl.col('age') >11))"
        },
        
        # 含麻疹成分疫苗第1针
        {
            'vaccine_name': '含麻疹成分疫苗',
            'vacc_code': '1201',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '8mo',
            'exclude_condition': "(pl.col('vaccine_name')=='含麻疹成分疫苗') & ((pl.col('age_month') <8) | (pl.col('age') >6))"
        },
        
        # 含麻疹成分疫苗第2针
        {
            'vaccine_name': '含麻疹成分疫苗',
            'vacc_code': '1201',
            'seq': 2,
            'based_on': {'vaccine': '含麻疹成分疫苗', 'seq': 1},
            'fallback_date': 'birth_date',
            'fallback_offset': '18mo',
            'interval': '1mo',
            'exclude_condition': "(pl.col('vaccine_name')=='含麻疹成分疫苗') & ((pl.col('age_month') <18) | (pl.col('age') >7))"
        },
        
        # A群流脑疫苗第1针
        {
            'vaccine_name': 'A群流脑疫苗',
            'vacc_code': '1601',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '6mo',
            'exclude_condition': "(pl.col('vaccine_name')=='A群流脑疫苗') & ((pl.col('age_month') <6) | (pl.col('age_month') >23))"
        },
        
        # A群流脑疫苗第2针
        {
            'vaccine_name': 'A群流脑疫苗',
            'vacc_code': '1601',
            'seq': 2,
            'calculation_logic': 'mav_2'
        },
        
        # A群C群流脑疫苗第1针
        {
            'vaccine_name': 'A群C群流脑疫苗',
            'vacc_code': '1701',
            'seq': 1,
            'calculation_logic': 'mac_1'
        },
        
        # A群C群流脑疫苗第2针
        {
            'vaccine_name': 'A群C群流脑疫苗',
            'vacc_code': '1701',
            'seq': 2,
            'calculation_logic': 'mac_2'
        },
        
        # 乙脑疫苗第1针
        {
            'vaccine_name': '乙脑疫苗',
            'vacc_code': '1801',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '8mo',
            'exclude_condition': "(pl.col('vaccine_name')=='乙脑疫苗') & ((pl.col('age_month') <8) | (pl.col('age') >6))"
        },
        
        # 乙脑疫苗第2针
        {
            'vaccine_name': '乙脑疫苗',
            'vacc_code': '1801',
            'seq': 2,
            'based_on': {'vaccine': '乙脑疫苗', 'seq': 1},
            'fallback_date': 'birth_date',
            'fallback_offset': '2y',
            'interval': '12mo',
            'exclude_condition': "(pl.col('vaccine_name')=='乙脑疫苗') & ((pl.col('age') <2) | (pl.col('age') >6))"
        },
        
        # 甲肝疫苗第1针
        {
            'vaccine_name': '甲肝疫苗',
            'vacc_code': '1901',
            'seq': 1,
            'base_date': 'birth_date',
            'offset': '18mo',
            'exclude_condition': "(pl.col('vaccine_name')=='甲肝疫苗') & ((pl.col('age_month') <18) | (pl.col('age') >6))"
        },
        
        # 甲肝疫苗第2针
        {
            'vaccine_name': '甲肝疫苗',
            'vacc_code': '1903',
            'seq': 2,
            'based_on': {'vaccine': '甲肝疫苗', 'seq': 1},
            'fallback_date': 'birth_date',
            'fallback_offset': '2y',
            'interval': '6mo',
            'exclude_condition': "(pl.col('vaccine_name')=='甲肝疫苗') & ((pl.col('age_month') <24) | (pl.col('age') >6))"
        }
    ]
    
    # 存储所有推荐结果
    all_recommendations = []
    
    for rule in vaccine_rules:
        try:
            # 根据规则类型处理
            if 'calculation_logic' in rule:
                # 特殊逻辑处理
                recommendation = _calculate_special_logic(person_data, rule)
            elif 'based_on' in rule:
                # 基于前一针计算
                recommendation = _calculate_based_on_previous(person_data, rule)
            else:
                # 基于出生日期计算
                recommendation = _calculate_from_birth(person_data, rule)
            
            if recommendation is not None and not recommendation.is_empty():
                all_recommendations.append(recommendation)
        except Exception as e:
            print(f"计算疫苗 {rule['vacc_code']} 第 {rule['seq']} 针时出错: {e}")
            continue
    
    # 合并所有推荐结果
    if all_recommendations:
        final_result = pl.concat(all_recommendations)
        return final_result.sort(["id_x", "recommended_vacc", "recommended_seq"])
    else:
        return pl.DataFrame()


def _calculate_from_birth(person_data, rule):
    """基于出生日期计算推荐时间"""
    result = (
        person_data
        .with_columns([
            pl.col(rule['base_date']).dt.offset_by(rule['offset']).alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
    )
    
    # 应用排除条件
    if 'exclude_condition' in rule:
        exclusion_expr = eval(rule['exclude_condition'])
        result = result.with_columns(
            recommended_dates=pl.when(exclusion_expr)
            .then(None)
            .otherwise(pl.col("recommended_dates"))
        )
    
    return result.group_by("id_x").agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])


def _calculate_based_on_previous(person_data, rule):
    """基于前一针接种时间计算推荐时间"""
    based_vaccine = rule['based_on']['vaccine']
    based_seq = rule['based_on']['seq']
    
    result = (
        person_data
        .with_columns([
            pl.when(
                (pl.col("vaccination_seq") == based_seq) & 
                (pl.col("vaccine_name") == based_vaccine)
            )
            .then(
                pl.max_horizontal([
                    pl.col(rule['fallback_date']).dt.offset_by(rule['fallback_offset']),
                    pl.col("vaccination_date").dt.offset_by(rule['interval'])
                ])
            )
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
    )
    
    # 应用排除条件
    if 'exclude_condition' in rule:
        exclusion_expr = eval(rule['exclude_condition'])
        result = result.with_columns(
            recommended_dates=pl.when(exclusion_expr)
            .then(None)
            .otherwise(pl.col("recommended_dates"))
        )
    
    return result.group_by("id_x").agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(),
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])


def _calculate_special_logic(person_data, rule):
    """处理特殊计算逻辑"""
    logic = rule['calculation_logic']
    
    if logic == 'hbv_2':
        return _calculate_hbv_2(person_data, rule)
    elif logic == 'hbv_3':
        return _calculate_hbv_3(person_data, rule)
    elif logic == 'hbv_4':
        return _calculate_hbv_4(person_data, rule)
    elif logic == 'mav_2':
        return _calculate_mav_2(person_data, rule)
    elif logic == 'mac_1':
        return _calculate_mac_1(person_data, rule)
    elif logic == 'mac_2':
        return _calculate_mac_2(person_data, rule)
    else:
        return None


def _calculate_hbv_2(person_data, rule):
    """乙肝疫苗第2针特殊逻辑"""
    return (
        person_data
        .with_columns([
            pl.when(
                (pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '乙肝疫苗')
            )
            .then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by("1mo"),
                    pl.col("vaccination_date").dt.offset_by("1mo")
                ])
            )
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .group_by("id_x")
        .agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


def _calculate_hbv_3(person_data, rule):
    """乙肝疫苗第3针特殊逻辑"""
    return (
        person_data
        .with_columns([
            # 检查是否存在第2剂乙肝疫苗记录
            ((pl.col("vaccination_seq") == 2) & 
             (pl.col("vaccine_name") == '乙肝疫苗'))
            .any()
            .over("id_x")
            .alias("has_dose2")
        ])
        .with_columns([
            # 基于第1剂的推荐时间计算
            pl.when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") < 1) &
                (pl.col("has_dose2"))
            )
            .then(pl.col("vaccination_date").dt.offset_by("6mo"))
            .when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("vaccine_name") == '乙肝疫苗') & 
                (pl.col("age") >= 1) &
                (pl.col("has_dose2"))
            )
            .then(pl.col("vaccination_date").dt.offset_by("4mo"))
            .otherwise(None)
            .alias("from_dose1"),
            
            # 基于第2剂的推荐时间
            pl.when(
                (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") < 1)
            )
            .then(pl.col("vaccination_date").dt.offset_by("1mo"))
            .when(
                (pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") >= 1)
            )
            .then(pl.col("vaccination_date").dt.offset_by("60d"))
            .otherwise(None)
            .alias("from_dose2"),
            
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .group_by("id_x")
        .agg([
            pl.max_horizontal([
                pl.col("from_dose1").drop_nulls().first(),
                pl.col("from_dose2").drop_nulls().first()
            ]).alias("recommended_dates"),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


def _calculate_hbv_4(person_data, rule):
    """乙肝疫苗第4针特殊逻辑"""
    return (
        person_data
        .with_columns([
            pl.when(
                (
                    (pl.col("hepatitis_mothers") == '1') |
                    ((pl.col("hepatitis_mothers") == '3') & (pl.col("birth_weight") < 2000))
                ) & 
                (pl.col("vaccination_seq") == 3) & 
                (pl.col("vaccine_name") == '乙肝疫苗')
            )
            .then(pl.col("vaccination_date").dt.offset_by("5mo"))
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .group_by("id_x")
        .agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


def _calculate_mav_2(person_data, rule):
    """A群流脑疫苗第2针特殊逻辑"""
    return (
        person_data
        .with_columns([
            pl.when(
                ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群流脑疫苗')) |
                ((pl.col("vaccination_seq") == 1) & (pl.col("vaccination_code").is_in(['1702', '1703', '1704'])))
            )
            .then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by("9mo"),
                    pl.col("vaccination_date").dt.offset_by("3mo")
                ])
            )
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .with_columns(
            recommended_dates=pl.when(
                (pl.col("vaccine_name")=='A群流脑疫苗') & ((pl.col("age_month") <9) | (pl.col("age_month") >23))
            )
            .then(None)
            .otherwise(pl.col("recommended_dates"))
        )
        .group_by("id_x")
        .agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


def _calculate_mac_1(person_data, rule):
    """A群C群流脑疫苗第1针特殊逻辑"""
    return (
        person_data
        .with_columns([
            ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
        ])
        .with_columns([
            pl.when((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == 'A群流脑疫苗'))
            .then(pl.col("birth_date").dt.offset_by("3y"))
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .with_columns(
            recommended_dates=pl.when(
                (pl.col("vaccine_name")=='A群C群流脑疫苗') & ((pl.col("age") < 3) | (pl.col("age") > 6))
            )
            .then(None)
            .otherwise(pl.col("recommended_dates"))
        )
        .with_columns([
            pl.when(
                (pl.col("vaccination_seq") == 1) & 
                (pl.col("vaccine_name") == 'A群流脑疫苗') &
                (pl.col("his_ma") == 1)
            )
            .then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by("3y"),
                    pl.col("vaccination_date").dt.offset_by("3mo")
                ])
            )
            .otherwise(pl.col("recommended_dates"))
            .alias("recommended_dates")
        ])
        .with_columns([
            pl.when(pl.col("his_ma") == 0)
            .then(pl.col("birth_date").dt.offset_by("3y"))
            .otherwise(pl.col("recommended_dates"))
            .alias("recommended_dates")
        ])
        .group_by("id_x")
        .agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


def _calculate_mac_2(person_data, rule):
    """A群C群流脑疫苗第2针特殊逻辑"""
    return (
        person_data
        .with_columns([
            pl.when(
                ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == 'A群C群流脑疫苗')) |
                ((pl.col("vaccination_seq") == 1) & (pl.col("vacc_month")>=24) & (pl.col("vaccination_code").is_in(['1702', '1703', '1704'])))
            )
            .then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by("5y"),
                    pl.col("vaccination_date").dt.offset_by("3y")
                ])
            )
            .otherwise(None)
            .alias("recommended_dates"),
            pl.lit(rule['vacc_code']).alias("recommended_vacc"),
            pl.lit(rule['seq']).alias('recommended_seq')
        ])
        .with_columns(
            recommended_dates=pl.when(
                (pl.col("vaccine_name")=='A群C群流脑疫苗') & (pl.col("age") <5)
            )
            .then(None)
            .otherwise(pl.col("recommended_dates"))
        )
        .group_by("id_x")
        .agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    )


# 使用示例
if __name__ == "__main__":
    # 准备数据
    person = (
        pl.read_csv('/mnt/d/标准库接种率/data/person2.csv',
                   schema_overrides={"vaccination_code":pl.Utf8,
                                   "birth_date":pl.Datetime,
                                   "vaccination_date":pl.Datetime,
                                   "hepatitis_mothers":pl.Utf8})
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
            ).alias('age')
        ])
        .with_columns([
            (
                (pl.col("vaccination_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
                (pl.col("vaccination_date").dt.month() - pl.col("birth_date").dt.month()) + 
                pl.when(pl.col("vaccination_date").dt.day() >= pl.col("birth_date").dt.day())
                .then(0)
                .otherwise(-1)
            ).alias('vacc_month')
        ])
        .with_columns([(
            (pl.col("expiration_date").dt.year() - pl.col("birth_date").dt.year()) * 12 +
            (pl.col("expiration_date").dt.month() - pl.col("birth_date").dt.month()) + 
            pl.when(pl.col("expiration_date").dt.day() >= pl.col("birth_date").dt.day())
            .then(0)
            .otherwise(-1)
        ).alias('age_month')])
        .join(vaccine_tbl, left_on="vaccination_code", right_on="小类编码", how='left')
        .with_columns(pl.col("vaccine_name").str.split(","))
        .explode("vaccine_name")
        .filter(pl.col("vaccination_date") <= pl.col("expiration_date"))
        .sort(["id_x", "vaccine_name", "vaccination_date"])
        .with_columns([
            pl.int_range(pl.len()).add(1).alias('vaccination_seq').over(["id_x", "vaccine_name"])
        ])
    )
    
    # 调用统一函数计算所有疫苗推荐时间
    recommendations = calculate_all_vaccine_recommendations(person)
