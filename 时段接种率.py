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


def calculate_MAV_actual_2(person: pl.DataFrame) -> pl.DataFrame:
    """
    计算A群流脑疫苗第2针的实种人数（特殊逻辑）
    
    包含三种情况：
    1. 直接接种A群流脑疫苗第2剂
    2. 第1剂是A群C群流脑疫苗(接种月龄<6)，第3剂A群C群流脑疫苗算作A群流脑疫苗第2剂
    3. 第1剂是A群C群流脑疫苗(接种月龄>=6)，第2剂A群C群流脑疫苗算作A群流脑疫苗第2剂
    """
    # 预先筛选基础数据
    base_person = person.filter(pl.col('age_month') < 2*12)
    
    # 符合条件的id集合 - p2: 第1剂AC群<6月龄
    eligible_ids_p2 = (
        base_person
        .filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 1) & 
            (pl.col('vacc_month') < 6)
        )
        .select('id_x')
        .unique()
    )
    
    # 符合条件的id集合 - p3: 第1剂AC群>=6月龄
    eligible_ids_p3 = (
        base_person
        .filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 1) & 
            (pl.col('vacc_month') >= 6)
        )
        .select('id_x')
        .unique()
    )
    
    parts_to_concat = []
    
    # p1: A群流脑疫苗 第2剂
    p1 = (
        base_person
        .filter(
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccine_name') == 'A群流脑疫苗') &
            (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
        )
        .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
        .agg(pl.col('id_x').n_unique().alias('vac'))
    )
    if p1.height > 0:
        parts_to_concat.append(p1)
    
    # p2: A群C群流脑疫苗 第3剂 -> 统计为 A群流脑疫苗第2剂
    if eligible_ids_p2.height > 0:
        base_p2 = base_person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 3) &
            (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
        )
        
        if base_p2.height > 0:
            p2 = (
                eligible_ids_p2.join(base_p2, on='id_x', how='inner')
                .with_columns([
                    pl.lit('A群流脑疫苗').alias('vaccine_name'),
                    pl.lit(2).cast(pl.Int64).alias('vaccination_seq')
                ])
                .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
                .agg(pl.col('id_x').n_unique().alias('vac'))
            )
            if p2.height > 0:
                parts_to_concat.append(p2)
    
    # p3: A群C群流脑疫苗 第2剂 -> 统计为 A群流脑疫苗第2剂
    if eligible_ids_p3.height > 0:
        base_p3 = base_person.filter(
            (pl.col('vaccine_name') == 'A群C群流脑疫苗') & 
            (pl.col('vaccination_seq') == 2) &
            (pl.col('vaccination_date').is_between(pl.col('mon_start'), pl.col('mon_end')))
        )
        
        if base_p3.height > 0:
            p3 = (
                eligible_ids_p3.join(base_p3, on='id_x', how='inner')
                .with_columns([
                    pl.lit('A群流脑疫苗').alias('vaccine_name'),
                    pl.lit(2).cast(pl.Int64).alias('vaccination_seq')
                ])
                .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
                .agg(pl.col('id_x').n_unique().alias('vac'))
            )
            if p3.height > 0:
                parts_to_concat.append(p3)
    
    # 合并结果
    if parts_to_concat:
        return (
            pl.concat(parts_to_concat)
            .group_by(['vaccination_org', 'vaccine_name', 'vaccination_seq'])
            .agg(pl.col('vac').sum().alias('vac'))
        )
    else:
        # 返回空DataFrame但保持schema一致
        return pl.DataFrame(
            schema={
                'vaccination_org': pl.Int64,
                'vaccine_name': pl.Utf8,
                'vaccination_seq': pl.Int64,
                'vac': pl.Int64
            }
        )


def calculate_MAV_expected_2(recommendations: pl.DataFrame, person: pl.DataFrame) -> pl.DataFrame:
    """  
    计算A群流脑疫苗第2针的应种人数
    
    参数:
        recommendations: 推荐接种数据框
        person: 个人接种记录数据框
    
    返回:
        pl.DataFrame: 符合条件的推荐接种记录（含应种人数统计）
    
    逻辑：
    - 排除已接种第2剂A群流脑疫苗的人群
    - 排除第1剂接种A群C群流脑疫苗时vacc_month<6，且已接种3剂次A群C群流脑疫苗的人群
    - 排除第1剂接种A群C群流脑疫苗时vacc_month>=6，且已接种2剂次A群C群流脑疫苗的人群
    - 筛选接种过第1剂且vacc_month<24的人群
    - 筛选推荐序列为2，推荐疫苗为A群流脑疫苗
    - 筛选推荐日期在范围内
    """
    result = (
        recommendations
        # 排除条件1：已接种第2剂A群流脑疫苗的人群
        .filter(
            ~pl.col('id_x').is_in(
                person
                .filter(
                    (pl.col('vaccination_seq') == 2) & 
                    (pl.col('vaccine_name') == 'A群流脑疫苗') & 
                    (pl.col("vaccination_date") <= pl.col('mon_end'))
                )
                ['id_x'].implode()
            )
        )
        # 排除条件2：第1剂接种A群C群流脑疫苗时vacc_month<6，且已接种3剂次A群C群流脑疫苗的人群
        .filter(
            ~pl.col('id_x').is_in(
                person
                .filter(
                    (pl.col('vaccination_seq') == 1) &
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                    (pl.col('vacc_month') < 6)
                )
                .select('id_x')
                .join(
                    person.filter(
                        (pl.col('vaccination_seq') == 3) &
                        (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                        (pl.col("vaccination_date") <= pl.col('mon_end'))
                    ).select('id_x'),
                    on='id_x',
                    how='inner'
                )
                ['id_x'].implode()
            )
        )
        # 排除条件3：第1剂接种A群C群流脑疫苗时vacc_month>=6，且已接种2剂次A群C群流脑疫苗的人群
        .filter(
            ~pl.col('id_x').is_in(
                person
                .filter(
                    (pl.col('vaccination_seq') == 1) &
                    (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                    (pl.col('vacc_month') >= 6)
                )
                .select('id_x')
                .join(
                    person.filter(
                        (pl.col('vaccination_seq') == 2) &
                        (pl.col('vaccine_name') == 'A群C群流脑疫苗') &
                        (pl.col("vaccination_date") <= pl.col('mon_end'))
                    ).select('id_x'),
                    on='id_x',
                    how='inner'
                )
                ['id_x'].implode()
            )
        )
        # 筛选条件：接种过第1剂且vacc_month<24的人群
        .filter(
            pl.col('id_x').is_in(
                person
                .filter(
                    (pl.col('vaccination_seq') == 1) & 
                    (pl.col('vacc_month') < 24) &
                    (
                        (pl.col('vaccine_name') == 'A群流脑疫苗') |
                        (pl.col('vaccine_name') == 'A群C群流脑疫苗')
                    ) &
                    (pl.col("vaccination_date") <= pl.col('mon_end'))
                )
                ['id_x'].implode()
            )
        )
        # 筛选推荐序列和疫苗类型
        .filter(
            (pl.col('recommended_seq') == 2) & 
            (pl.col('recommended_vacc') == 'A群流脑疫苗')
        )
        # 筛选推荐日期范围
        .filter(
            (pl.col('recommended_dates').dt.date() <= pl.col('mon_end')) & 
            (pl.col('recommended_dates').dt.date() >= pl.col('mon_start').dt.offset_by("-1y"))
        )
        .group_by(['current_management_code', 'recommended_vacc', 'recommended_seq'])
        .agg(pl.col('id_x').n_unique().alias('exp'))
    )
    
    return result

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
    # 如果actual和expected都为空，返回空结果
    if actual.height == 0 and expected.height == 0:
        return pl.DataFrame(
            schema={
                'org_code': pl.Int64,
                'vacc_name': pl.Utf8,
                'seq': pl.Int64,
                'vac': pl.Int64,
                'exp': pl.Int64,
                'percent': pl.Float64
            }
        )
    
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
    
    # 统一列名和数据类型
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
    
    # 添加统计列
    standardized_columns.extend([
        pl.col('vac').cast(pl.Int64).alias('vac'),
        pl.col('exp').cast(pl.Int64).alias('exp'),
        pl.col('percent').cast(pl.Float64).alias('percent')
    ])
    
    return result.select(standardized_columns)


def calculate_vaccine_coverage_for_all_doses(
    person: pl.DataFrame,
    recommendations: pl.DataFrame,
    vaccine_name: str,
    dose_configs: List[Tuple[int, int, int, int, bool]]
) -> List[pl.DataFrame]:
    """
    计算某个疫苗所有剂次的接种率
    
    Args:
        person: 接种记录数据框
        recommendations: 推荐接种数据框
        vaccine_name: 疫苗名称
        dose_configs: 剂次配置列表 [(序号, 实种最大年龄, 应种最大年龄, 应种最小年龄, 是否需要前一针)]
    
    Returns:
        各剂次接种率数据框列表
    """
    coverage_list = []
    
    for seq, actual_max_age, expected_max_age, expected_min_age, require_prev in dose_configs:
        try:
            # A群流脑疫苗第2针使用特殊逻辑
            if vaccine_name == 'A群流脑疫苗' and seq == 2:
                print(f"  使用特殊逻辑计算 {vaccine_name} 第{seq}剂...")
                actual = calculate_MAV_actual_2(person)
                expected = calculate_MAV_expected_2(recommendations, person)
                print(f"  实种记录数: {actual.height}, 应种记录数: {expected.height}")
            else:
                # 计算实种
                actual = calculate_actual_vaccination(person, vaccine_name, seq, actual_max_age)
                
                # 计算应种
                expected = calculate_expected_vaccination(
                    recommendations, person, vaccine_name, seq, 
                    expected_max_age, expected_min_age, require_prev
                )
            
            # 根据原代码，乙肝疫苗第1针使用 inner join，其他使用 right join
            join_how = 'inner' if (vaccine_name == '乙肝疫苗' and seq == 1) else 'right'
            
            # 计算接种率
            coverage = calculate_coverage(actual, expected, join_how=join_how)
            
            if coverage.height > 0:
                coverage_list.append(coverage)
            else:
                print(f"  警告: {vaccine_name} 第{seq}剂 计算结果为空")
                
        except Exception as e:
            print(f"  错误: 计算 {vaccine_name} 第{seq}剂时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    return coverage_list


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
                    
            print(f"  {vaccine_name} 完成，共 {len(coverage_list)} 个剂次有数据")
            
        except Exception as e:
            print(f"计算 {vaccine_name} 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    # 合并所有结果
    if all_coverage:
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
        print("警告: 没有计算出任何接种率数据")
        return pl.DataFrame()


# 使用示例
if __name__ == "__main__":
    # 计算所有疫苗的接种率
    all_vaccine_coverage = calculate_all_vaccines_coverage(person, recommendations)
    

    
    # 查看特定接种单位的数据
    tmp = (
        all_vaccine_coverage
        .filter(pl.col('接种单位') == 392423210604)
    )
