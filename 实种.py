import polars as pl
from typing import List, Tuple

# 疫苗配置：(疫苗名称, [(接种序号, 实种最大年龄(月), 应种最大年龄(月), 应种最小年龄(月), 是否需要前一针)]
VACCINE_CONFIGS = {
    '卡介苗': [
        (1, 4*12, 4*12, 0, False)
    ],
    '乙肝疫苗': [
        (1, 18*12, 6*12, 0, False),
        (2, 18*12, 6*12, 0, True),
        (3, 18*12, 6*12, 0, True)
    ],
    '脊灰疫苗': [
        (1, 18*12, 6*12, 2, False),
        (2, 18*12, 6*12, 3, True),
        (3, 18*12, 6*12, 4, True),
        (4, 18*12, 6*12, 4*12, True)
    ],
    '百白破疫苗': [
        (1, 18*12, 6*12, 2, False),
        (2, 18*12, 6*12, 4, True),
        (3, 18*12, 6*12, 6, True),
        (4, 18*12, 6*12, 18, True), 
        (5, 18*12, 7*12, 6*12, True)
    ],
    '白破疫苗': [
        (1, 12*12, 11*12, 6*12, False)
    ],
    '含麻疹成分疫苗': [
        (1, 18*12, 6*12, 8, False),
        (2, 18*12, 6*12, 18, True)
    ],
    'A群流脑疫苗': [
        (1, 2*12, 23, 6, False),
        (2, 2*12, 23, 9, True)
    ],
    'A群C群流脑疫苗': [
        (1, 18*12, 6*12, 2*12, False),
        (2, 18*12, 8*12, 5*12, True)
    ],
    '乙脑疫苗': [
        (1, 18*12, 6*12, 8, False),
        (2, 18*12, 6*12, 2*12, True)
    ],
    '甲肝疫苗': [
        (1, 18*12, 6*12, 18, False)
    ]
}


def calculate_actual_vaccination(
    person: pl.DataFrame,
    vaccine_name: str,
    seq: int,
    max_age_months: int
) -> pl.DataFrame:
    """
    计算实种人数（当月，按接种单位）
    
    Args:
        person: 接种记录数据框
        vaccine_name: 疫苗名称
        seq: 接种序号
        max_age_months: 最大年龄（月）
    
    Returns:
        实种统计数据框
    """
    return (
        person
        .filter(
            (pl.col('age_month') < max_age_months) & 
            (pl.col('vaccination_seq') == seq) &
            (pl.col('vaccine_name') == vaccine_name) &
            (pl.col('vaccination_date') >= pl.col('mon_start')) & 
            (pl.col('vaccination_date') <= pl.col('mon_end'))
        )
        .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
        .agg(pl.col('id_x').n_unique().alias('vac'))
    )


def get_vaccinated_ids(
    person: pl.DataFrame,
    vaccine_name: str,
    seq: int
) -> pl.Expr:
    """
    获取已接种指定剂次的人员ID列表
    
    Args:
        person: 接种记录数据框
        vaccine_name: 疫苗名称
        seq: 接种序号
    
    Returns:
        ID列表表达式
    """
    return (
        person
        .filter(
            (pl.col('vaccination_seq') == seq) & 
            (pl.col('vaccine_name') == vaccine_name) & 
            (pl.col('vaccination_date') <= pl.col('mon_end'))
        )
        ['id_x']
        .implode()
    )


def calculate_expected_vaccination(
    recommendations: pl.DataFrame,
    person: pl.DataFrame,
    vaccine_name: str,
    seq: int,
    max_age_months: int,
    min_age_months: int,
    require_previous_dose: bool
) -> pl.DataFrame:
    """
    计算应种人数（当月-1年，按管理单位）
    
    Args:
        recommendations: 推荐接种数据框
        person: 接种记录数据框
        vaccine_name: 疫苗名称
        seq: 接种序号
        max_age_months: 应种最大年龄（月）
        min_age_months: 应种最小年龄（月）
        require_previous_dose: 是否需要完成前一剂次
    
    Returns:
        应种统计数据框
    """
    # 排除已接种当前剂次的人员
    df = recommendations.filter(
        ~pl.col('id_x').is_in(get_vaccinated_ids(person, vaccine_name, seq))
    )
    
    # 如果需要前一剂次，则只包含已完成前一剂次的人员
    if require_previous_dose:
        df = df.filter(
            pl.col('id_x').is_in(get_vaccinated_ids(person, vaccine_name, seq - 1))
        )
    
   
    df = df.filter(
        (pl.col('age_month') <= max_age_months) & 
        (pl.col('age_month') >= min_age_months) &
        (pl.col('recommended_seq') == seq) & 
        (pl.col('recommended_vacc') == vaccine_name)
    )
    
    # 时间范围过滤
    df = df.filter(
        (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
        (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
    )
    
    # 分组统计
    return (
        df
        .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
        .agg(pl.col('id_x').n_unique().alias('exp'))
    )


def calculate_coverage(
    actual: pl.DataFrame,
    expected: pl.DataFrame,
    join_how: str = 'right'
) -> pl.DataFrame:
    """
    计算接种率
    
    Args:
        actual: 实种数据框
        expected: 应种数据框
        join_how: 连接方式，默认为'right'
    
    Returns:
        接种率数据框
    """
    result = (
        actual
        .join(
            expected,
            left_on=['vaccination_org', 'vaccine_name', 'vaccination_seq'],
            right_on=['current_management_code', 'recommended_vacc', 'recommended_seq'],
            how=join_how
        )
        .with_columns(pl.col('vac').fill_null(0))
        .with_columns(
            (pl.col('vac') / (pl.col('vac') + pl.col('exp')) * 100).alias('percent')
        )
    )
    
    # 统一列名和数据类型，确保所有返回的 DataFrame 具有相同的列结构
    standardized_columns = []
    
    # 处理机构代码列
    if 'vaccination_org' in result.columns:
        standardized_columns.append(pl.col('vaccination_org').alias('org_code'))
    elif 'current_management_code' in result.columns:
        standardized_columns.append(pl.col('current_management_code').alias('org_code'))
    else:
        raise ValueError("找不到机构代码列")
    
    # 处理疫苗名称列
    if 'vaccine_name' in result.columns:
        standardized_columns.append(pl.col('vaccine_name').alias('vacc_name'))
    elif 'recommended_vacc' in result.columns:
        standardized_columns.append(pl.col('recommended_vacc').alias('vacc_name'))
    else:
        raise ValueError("找不到疫苗名称列")
    
    # 处理接种序号列 - 统一转换为 Int64
    if 'vaccination_seq' in result.columns:
        standardized_columns.append(pl.col('vaccination_seq').cast(pl.Int64).alias('seq'))
    elif 'recommended_seq' in result.columns:
        standardized_columns.append(pl.col('recommended_seq').cast(pl.Int64).alias('seq'))
    else:
        raise ValueError("找不到接种序号列")
    
    # 添加统计列 - 统一数据类型
    standardized_columns.extend([
        pl.col('vac').cast(pl.Int64).alias('vac'),
        pl.col('exp').cast(pl.Int64).alias('exp'),
        pl.col('percent').cast(pl.Float64).alias('percent')
    ])
    
    return result.select(standardized_columns)


def calculate_all_vaccines_coverage(
    person: pl.DataFrame,
    recommendations: pl.DataFrame,
    vaccine_configs: dict = VACCINE_CONFIGS
) -> pl.DataFrame:
    """
    计算所有疫苗的接种率并合并
    
    Args:
        person: 接种记录数据框
        recommendations: 推荐接种数据框
        vaccine_configs: 疫苗配置字典
    
    Returns:
        合并后的所有疫苗接种率统计
    """
    all_coverage = []
    
    for vaccine_name, dose_configs in vaccine_configs.items():
        print(f"正在计算 {vaccine_name} 的接种率...")
        
        try:
            coverage_list = calculate_vaccine_coverage_for_all_doses(
                person, recommendations, vaccine_name, dose_configs
            )
            
            # 检查每个返回的 DataFrame 是否为空
            for cov in coverage_list:
                if cov.height > 0:  # 只添加非空的 DataFrame
                    all_coverage.append(cov)
        except Exception as e:
            print(f"计算 {vaccine_name} 时出错: {str(e)}")
            continue
    
    # 合并所有结果
    if all_coverage:
        # 在合并前，再次确认所有 DataFrame 的列类型
        # 打印第一个 DataFrame 的 schema 用于调试
        print("第一个 DataFrame 的 schema:")
        print(all_coverage[0].schema)
        
        # 现在所有 DataFrame 都有相同的列结构和数据类型，可以安全地合并
        combined_coverage = pl.concat(all_coverage, how='vertical')
        
        # 重命名为最终的列名
        combined_coverage = combined_coverage.select([
            pl.col('org_code').alias('接种单位'),
            pl.col('vacc_name').alias('疫苗名称'),
            pl.col('seq').alias('剂次'),
            pl.col('vac').alias('实种人数'),
            pl.col('exp').alias('应种人数'),
            pl.col('percent').round(2).alias('接种率(%)')
        ])
        
        return combined_coverage
    else:
        return pl.DataFrame()


# 使用示例
if __name__ == "__main__":
    # 计算所有疫苗的接种率
    all_vaccine_coverage = calculate_all_vaccines_coverage(person, recommendations)

    (
        all_vaccine_coverage
        .filter(pl.col('接种单位')==392423210604)
    )
