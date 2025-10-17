import polars as pl
from datetime import datetime
from typing import Dict, Any, List, Optional, Union

def calculate_all_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
    """
    统一计算所有疫苗的推荐时间
  
    Args:
        person: 包含个人信息和疫苗接种记录的DataFrame
      
    Returns:
        包含所有疫苗推荐时间的DataFrame
    """
  
    # 疫苗推荐规则配置
    vaccine_configs = [
        # 卡介苗
        {
            "vaccine_name": "卡介苗",
            "vaccine_category": "卡介苗", 
            "dose": 1,
            "base_schedule": "0d",  # 出生时
            "dependency": None,
            "special_conditions": None
        },
      
        # 乙肝疫苗系列
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 1,
            "base_schedule": "0d",  # 出生时
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 2,
            "base_schedule": "1mo",  # 出生后1个月
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_conditions": None
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗", 
            "dose": 3,
            "base_schedule": None,
            "dependency": {"prev_dose": 2, "age_based_interval": True},
            "special_conditions": "hbv_dose3"
        },
        {
            "vaccine_name": "乙肝疫苗",
            "vaccine_category": "乙肝疫苗",
            "dose": 4,
            "base_schedule": None,
            "dependency": {"prev_dose": 3, "min_interval": "5mo"},
            "special_conditions": "high_risk_hbv"
        },
      
        # 脊灰疫苗系列
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 1,
            "base_schedule": "2mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "脊灰疫苗", 
            "vaccine_category": "脊灰疫苗",
            "dose": 2,
            "base_schedule": "3mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_conditions": None
        },
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗", 
            "dose": 3,
            "base_schedule": "4mo",
            "dependency": {"prev_dose": 2, "min_interval": "1mo"},
            "special_conditions": None
        },
        {
            "vaccine_name": "脊灰疫苗",
            "vaccine_category": "脊灰疫苗",
            "dose": 4, 
            "base_schedule": "4y",
            "dependency": {"prev_dose": 3, "min_interval": "1mo"},
            "special_conditions": None
        },
      
        # 百白破疫苗系列
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 1,
            "base_schedule": "2mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 2,
            "base_schedule": "4mo", 
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_conditions": None
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 3,
            "base_schedule": "6mo",
            "dependency": {"prev_dose": 2, "min_interval": "1mo"}, 
            "special_conditions": None
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗",
            "dose": 4,
            "base_schedule": "18mo",
            "dependency": {"prev_dose": 3, "min_interval": "1mo"},
            "special_conditions": None
        },
        {
            "vaccine_name": "百白破疫苗",
            "vaccine_category": "百白破疫苗", 
            "dose": 5,
            "base_schedule": "6y",
            "dependency": {"prev_dose": 4, "min_interval": "1mo"},
            "special_conditions": None
        },
      
        # 白破疫苗
        {
            "vaccine_name": "白破疫苗",
            "vaccine_category": "白破疫苗",
            "dose": 1,
            "base_schedule": "7y", 
            "dependency": None,
            "special_conditions": None
        },
      
        # 含麻疹成分疫苗系列
        {
            "vaccine_name": "含麻疹成分疫苗",
            "vaccine_category": "含麻疹成分疫苗",
            "dose": 1,
            "base_schedule": "8mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "含麻疹成分疫苗",
            "vaccine_category": "含麻疹成分疫苗",
            "dose": 2,
            "base_schedule": "18mo",
            "dependency": {"prev_dose": 1, "min_interval": "1mo"},
            "special_conditions": None
        },
      
        # A群流脑疫苗系列
        {
            "vaccine_name": "A群流脑疫苗",
            "vaccine_category": "A群流脑疫苗", 
            "dose": 1,
            "base_schedule": "6mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "A群C群流脑疫苗",
            "vaccine_category": "A群流脑疫苗",
            "dose": 2,
            "base_schedule": "9mo",
            "dependency": {"prev_dose": 1, "min_interval": "3mo", "prev_vaccine": "A群C群流脑疫苗"},
            "special_conditions": None
        },
      
        # A群C群流脑疫苗系列
        {
            "vaccine_name": "A群C群流脑疫苗",
            "vaccine_category": "A群C群流脑疫苗",
            "dose": 1,
            "base_schedule": "2y",
            "dependency": None,
            "special_conditions": "mac_dose1"
        },
        {
            "vaccine_name": "A群C群流脑疫苗", 
            "vaccine_category": "A群C群流脑疫苗",
            "dose": 2,
            "base_schedule": "6y",
            "dependency": {"prev_dose": 1, "min_interval": "3y"},
            "special_conditions": None
        },
      
        # 乙脑疫苗系列
        {
            "vaccine_name": "乙脑疫苗",
            "vaccine_category": "乙脑疫苗",
            "dose": 1,
            "base_schedule": "8mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "乙脑疫苗",
            "vaccine_category": "乙脑疫苗",
            "dose": 2, 
            "base_schedule": "2y",
            "dependency": {"prev_dose": 1, "min_interval": "12mo"},
            "special_conditions": None
        },
      
        # 甲肝疫苗系列
        {
            "vaccine_name": "甲肝疫苗",
            "vaccine_category": "甲肝疫苗",
            "dose": 1,
            "base_schedule": "18mo",
            "dependency": None,
            "special_conditions": None
        },
        {
            "vaccine_name": "甲肝疫苗",
            "vaccine_category": "甲肝疫苗",
            "dose": 2,
            "base_schedule": "2y",
            "dependency": {"prev_dose": 1, "min_interval": "6mo"},
            "special_conditions": "hav_inactivated"
        }
    ]
  
    all_recommendations = []
  
    for config in vaccine_configs:
        try:
            recommendation = _calculate_single_vaccine_recommendation(person, config)
            if recommendation is not None and recommendation.height > 0:
                all_recommendations.append(recommendation)
        except Exception as e:
            print(f"计算疫苗推荐时间时出错 - {config['vaccine_name']} 第{config['dose']}剂: {e}")
            continue
  
    # 合并所有推荐结果
    if all_recommendations:
        return pl.concat(all_recommendations, how="vertical")
    else:
        return pl.DataFrame()

def _calculate_single_vaccine_recommendation(person: pl.DataFrame, config: Dict[str, Any]) -> Optional[pl.DataFrame]:
    """
    计算单个疫苗推荐时间的核心逻辑
  
    Args:
        person: 个人疫苗接种数据
        config: 疫苗配置信息
      
    Returns:
        单个疫苗推荐结果DataFrame
    """
  
    vaccine_name = config["vaccine_name"]
    vaccine_category = config["vaccine_category"] 
    dose = config["dose"]
    base_schedule = config["base_schedule"]
    dependency = config.get("dependency")
    special_conditions = config.get("special_conditions")
  
    # 开始计算推荐时间
    df = person.clone()
  
    # 计算基础推荐时间
    recommended_date_expr = _get_recommended_date_expression(
        base_schedule, dependency, special_conditions
    )
  
    df = df.with_columns([
        recommended_date_expr.alias("recommended_dates"),
        pl.lit(vaccine_name).alias("recommended_vacc"),
        pl.lit(dose).alias("recommended_seq")
    ])
  
    # 应用疫苗接种状态检查逻辑
    df = df.with_columns(
        _get_vaccination_status_check(vaccine_category, dose).alias("recommended_dates")
    )
  
    # 按人员ID聚合结果
    result = df.group_by("id_x").agg([
        pl.col("recommended_dates").drop_nulls().first(),
        pl.col("recommended_vacc").first(),
        pl.col("recommended_seq").first(), 
        pl.col("birth_date").first(),
        pl.col("age").first(),
        pl.col("entry_org").first(),
        pl.col("entry_date").first(),
        pl.col("current_management_code").first()
    ])
  
    return result

def _get_recommended_date_expression(base_schedule: Optional[str], 
                                   dependency: Optional[Dict], 
                                   special_conditions: Optional[str]) -> pl.Expr:
    """
    根据配置生成推荐日期计算表达式
  
    Args:
        base_schedule: 基础接种计划(如"2mo", "1y"等)
        dependency: 依赖前一剂的配置
        special_conditions: 特殊条件标识
      
    Returns:
        polars表达式用于计算推荐日期
    """
  
    if special_conditions == "hbv_dose3":
        # 乙肝疫苗第3剂特殊逻辑
        return pl.when(
            ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") < 1)) |
            ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") < 1) &
             ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗')).any().over("id_x"))
        ).then(
            pl.max_horizontal([
                pl.col("vaccination_date").dt.offset_by("1mo"),   # 与第2剂间隔1个月
                pl.col("vaccination_date").shift().dt.offset_by("6mo")  # 与第1剂间隔6个月
            ])
        ).when(
            ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") >= 1)) |
            ((pl.col("vaccination_seq") == 1) & (pl.col("vaccine_name") == '乙肝疫苗') & (pl.col("age") >= 1) &
             ((pl.col("vaccination_seq") == 2) & (pl.col("vaccine_name") == '乙肝疫苗')).any().over("id_x"))
        ).then(
            pl.max_horizontal([
                pl.col("vaccination_date").dt.offset_by("60d"),   # 与第2剂间隔60天
                pl.col("vaccination_date").shift().dt.offset_by("4mo")  # 与第1剂间隔4个月
            ])
        ).otherwise(None)
      
    elif special_conditions == "high_risk_hbv":
        # 乙肝疫苗第4剂(高危人群)
        return pl.when(
            ((pl.col("hepatitis_mothers") == '1') | 
             ((pl.col("hepatitis_mothers") == '3') & (pl.col("birth_weight") < 2000))) &
            (pl.col("vaccination_seq") == 3) & (pl.col("vaccine_name") == '乙肝疫苗')
        ).then(pl.col("vaccination_date").dt.offset_by("5mo")).otherwise(None)
      
    elif special_conditions == "mac_dose1":
        # A群C群流脑疫苗第1剂特殊逻辑
        return pl.when(
            # 如果已接种2剂A群流脑疫苗
            ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x") == 2
        ).then(pl.col("birth_date").dt.offset_by("3y")).when(
            # 如果已接种1剂A群流脑疫苗
            ((pl.col("vaccine_name") == 'A群流脑疫苗') & (pl.col("age") >= 2)).sum().over("id_x") == 1
        ).then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("2y"),
                pl.col("vaccination_date").dt.offset_by("3mo")
            ])
        ).otherwise(pl.col("birth_date").dt.offset_by("2y"))
        
    elif special_conditions == "hav_inactivated":
        # 甲肝灭活疫苗第2剂
        return pl.when(
            (pl.col("vaccination_seq") == 1) & (pl.col("小类名称") == '甲型肝炎灭活疫苗（人二倍体细胞）')
        ).then(
            pl.max_horizontal([
                pl.col("birth_date").dt.offset_by("2y"),
                pl.col("vaccination_date").dt.offset_by("6mo")
            ])
        ).otherwise(None)
        
    elif dependency:
        # 基于前一剂的依赖逻辑
        prev_dose = dependency.get("prev_dose", 1)
        min_interval = dependency.get("min_interval", "1mo")
        prev_vaccine = dependency.get("prev_vaccine")
        age_based_interval = dependency.get("age_based_interval", False)
        
        if age_based_interval:
            # 年龄相关的间隔时间(如乙肝疫苗第3剂)
            return pl.when(
                (pl.col("vaccination_seq") == prev_dose) & 
                (pl.col("vaccine_name") == (prev_vaccine or pl.col("vaccine_name"))) &
                (pl.col("age") < 1)
            ).then(pl.col("vaccination_date").dt.offset_by("6mo")).when(
                (pl.col("vaccination_seq") == prev_dose) & 
                (pl.col("vaccine_name") == (prev_vaccine or pl.col("vaccine_name"))) &
                (pl.col("age") >= 1)
            ).then(pl.col("vaccination_date").dt.offset_by("4mo")).otherwise(None)
        else:
            # 标准依赖逻辑
            return pl.when(
                (pl.col("vaccination_seq") == prev_dose) & 
                (pl.col("vaccine_name") == (prev_vaccine or pl.col("vaccine_name")))
            ).then(
                pl.max_horizontal([
                    pl.col("birth_date").dt.offset_by(base_schedule) if base_schedule else pl.col("vaccination_date").dt.offset_by("0d"),
                    pl.col("vaccination_date").dt.offset_by(min_interval)
                ])
            ).otherwise(None)
    else:
        # 基础时间计划
        if base_schedule:
            return pl.col("birth_date").dt.offset_by(base_schedule)
        else:
            return pl.lit(None)

def _get_vaccination_status_check(vaccine_category: str, dose: int) -> pl.Expr:
    """
    生成疫苗接种状态检查逻辑
    
    Args:
        vaccine_category: 疫苗大类名称
        dose: 剂次
        
    Returns:
        polars表达式用于检查是否需要推荐
    """
    
    return pl.when(
        # 如果当前推荐剂次已接种且接种日期晚于推荐日期，不推荐
        (pl.col('recommended_seq') == dose) & 
        (pl.col("大类名称") == vaccine_category) &
        (pl.col('vaccination_seq') == dose) &
        (pl.col('vaccination_date') > pl.col('recommended_dates'))
    ).then(pl.col('recommended_dates')).when(
        # 如果当前推荐剂次已接种但接种日期早于推荐日期，推荐补种
        (pl.col('recommended_seq') == dose) & 
        (pl.col("大类名称") == vaccine_category) &
        (pl.col('vaccination_seq') < dose)
    ).then(pl.col('recommended_dates')).when(
        # 如果没有接种此类疫苗，推荐接种
        ~(pl.col("大类名称").is_in([vaccine_category]))
    ).then(pl.col('recommended_dates')).otherwise(None)

def get_consolidated_vaccine_recommendations(person: pl.DataFrame) -> pl.DataFrame:
    """
    获取合并后的所有疫苗推荐结果
    
    Args:
        person: 包含个人信息和疫苗接种记录的DataFrame
        
    Returns:
        包含所有疫苗推荐时间的合并DataFrame
    """
    
    # 计算所有疫苗推荐时间
    all_recommendations = calculate_all_vaccine_recommendations(person)
    
    if all_recommendations.height == 0:
        return pl.DataFrame()
    
    # 过滤掉空的推荐日期
    valid_recommendations = all_recommendations.filter(
        pl.col("recommended_dates").is_not_null()
    )
    
    # 按推荐日期排序
    sorted_recommendations = valid_recommendations.sort(
        ["id_x", "recommended_dates", "recommended_vacc", "recommended_seq"]
    )
    
    return sorted_recommendations

# 辅助函数
def get_recommendations_by_vaccine(person: pl.DataFrame, vaccine_name: str) -> pl.DataFrame:
    """
    获取特定疫苗的推荐时间
    
    Args:
        person: 个人疫苗数据
        vaccine_name: 疫苗名称
        
    Returns:
        特定疫苗的推荐结果
    """
    all_recommendations = get_consolidated_vaccine_recommendations(person)
    
    return all_recommendations.filter(
        pl.col("recommended_vacc") == vaccine_name
    )

def get_recommendations_by_person(person: pl.DataFrame, person_id: str) -> pl.DataFrame:
    """
    获取特定人员的所有疫苗推荐
    
    Args:
        person: 个人疫苗数据
        person_id: 人员ID
        
    Returns:
        特定人员的推荐结果
    """
    all_recommendations = get_consolidated_vaccine_recommendations(person)
    
    return all_recommendations.filter(
        pl.col("id_x") == person_id
    ).sort("recommended_dates")

def get_overdue_recommendations(person: pl.DataFrame, current_date: str = None) -> pl.DataFrame:
    """
    获取超期未种的疫苗推荐
    
    Args:
        person: 个人疫苗数据
        current_date: 当前日期，默认为今天
        
    Returns:
        超期未种的推荐结果
    """
    if current_date is None:
        current_date = datetime.now().strftime('%Y-%m-%d')
    
    all_recommendations = get_consolidated_vaccine_recommendations(person)
    
    # 修复日期解析方法
    return all_recommendations.filter(
        pl.col("recommended_dates") < pl.lit(current_date).str.to_date()
    ).sort(["id_x", "recommended_dates"])

def export_recommendations_to_excel(recommendations: pl.DataFrame, filename: str):
    """
    将推荐结果导出到Excel文件
    
    Args:
        recommendations: 推荐结果DataFrame
        filename: 输出文件名
    """
    try:
        # 转换为pandas DataFrame并导出
        recommendations.to_pandas().to_excel(filename, index=False)
        print(f"推荐结果已导出到文件: {filename}")
    except Exception as e:
        print(f"导出文件时出错: {e}")

# 数据验证函数
def validate_person_data(person: pl.DataFrame) -> bool:
    """
    验证person数据是否包含必要字段
    
    Args:
        person: 待验证的DataFrame
        
    Returns:
        验证结果
    """
    required_fields = [
        "id_x", "birth_date", "age", "大类名称", "vaccine_name", 
        "vaccination_seq", "vaccination_date", "entry_org", 
        "entry_date", "current_management_code"
    ]
    
    missing_fields = []
    for field in required_fields:
        if field not in person.columns:
            missing_fields.append(field)
    
    if missing_fields:
        print(f"缺少必要字段: {missing_fields}")
        return False
    
    print("数据验证通过")
    return True

# 使用示例
if __name__ == "__main__":
    # 假设person DataFrame已经准备好
    # person = pl.read_csv("person_vaccination_data.csv")
    
    # 计算所有疫苗推荐时间
    recommendations = get_consolidated_vaccine_recommendations(person)

    # 获取特定疫苗推荐
    hbv_recommendations = get_recommendations_by_vaccine(person, "乙肝疫苗")
    # 获取特定人员推荐
    person_recommendations = get_recommendations_by_person(person, "0e09122b2d9e4e2e9726faa0eb65d639")
    # 获取超期推荐
    overdue = get_overdue_recommendations(person)
