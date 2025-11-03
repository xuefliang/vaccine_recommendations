import polars as pl
from datetime import datetime
from typing import Optional

def load_and_process_person_data(
    file_path: str,
    cutoff_date: str,
    vaccine_tbl: pl.DataFrame,
    replacement_org: str = "777777777777",
    target_vaccines: Optional[list] = None
) -> pl.DataFrame:
    """
    读取并处理人员疫苗接种数据
    
    Args:
        file_path: CSV文件路径
        cutoff_date: 截止日期 (格式: 'YYYY-MM-DD')
        vaccine_tbl: 疫苗编码对照表 DataFrame
        replacement_org: 需要替换的机构代码，默认777777777777
        target_vaccines: 需要特殊处理的疫苗列表，默认['乙肝疫苗','卡介苗']
    
    Returns:
        处理后的 DataFrame
    """
    if target_vaccines is None:
        target_vaccines = ['乙肝疫苗', '卡介苗']
    
    # 定义schema
    schema_overrides = {
        "vaccination_code": pl.String,
        "birth_date": pl.Datetime,
        "vaccination_date": pl.Datetime,
        "hepatitis_mothers": pl.String,
        "current_management_code":pl.String,
        "vaccination_org": pl.String,
        "entry_org": pl.String
    }
    
    person = (
        # 1. 读取数据
        pl.read_csv(file_path, schema_overrides=schema_overrides)
        
        # 2. 添加时间相关列
        .pipe(_add_date_columns, cutoff_date)
        
        # 3. 计算年龄
        .pipe(_calculate_age)
        
        # 4. 计算接种月龄和当前月龄
        .pipe(_calculate_months)
        
        # 5. 关联疫苗信息并展开
        .pipe(_join_and_explode_vaccines, vaccine_tbl)
        
        # 6. 添加接种序次
        .pipe(_add_vaccination_sequence)
        
        # 7. 修正接种机构
        .pipe(_fix_vaccination_org, replacement_org, target_vaccines)
    )
    
    return person


def _add_date_columns(df: pl.DataFrame, cutoff_date: str) -> pl.DataFrame:
    """添加截止日期相关列"""
    cutoff_dt = pl.lit(cutoff_date).str.to_date()
    
    return df.with_columns([
        cutoff_dt.dt.month_end().alias('expiration_date'),
        cutoff_dt.dt.month_start().alias('mon_start'),
        cutoff_dt.dt.month_end().alias('mon_end')
    ])


def _calculate_age(df: pl.DataFrame) -> pl.DataFrame:
    """计算周岁年龄"""
    return df.with_columns([
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


def _calculate_months(df: pl.DataFrame) -> pl.DataFrame:
    """计算接种月龄和当前月龄"""
    def _month_diff(end_col: str, start_col: str) -> pl.Expr:
        """计算两个日期之间的月份差"""
        return (
            (pl.col(end_col).dt.year() - pl.col(start_col).dt.year()) * 12 +
            (pl.col(end_col).dt.month() - pl.col(start_col).dt.month()) +
            pl.when(pl.col(end_col).dt.day() >= pl.col(start_col).dt.day())
            .then(0)
            .otherwise(-1)
        )
    
    return df.with_columns([
        _month_diff("vaccination_date", "birth_date").alias('vacc_month'),
        _month_diff("expiration_date", "birth_date").alias('age_month')
    ])


def _join_and_explode_vaccines(df: pl.DataFrame, vaccine_tbl: pl.DataFrame) -> pl.DataFrame:
    """关联疫苗表并展开疫苗名称"""
    return (
        df.join(vaccine_tbl, left_on="vaccination_code", right_on="小类编码", how='left')
        .with_columns(pl.col("vaccine_name").str.split(","))
        .explode("vaccine_name")
        .sort(["id_x", "vaccine_name", "vaccination_date"])
    )


def _add_vaccination_sequence(df: pl.DataFrame) -> pl.DataFrame:
    """为每个人每种疫苗添加接种序次"""
    return df.with_columns([
        pl.int_range(pl.len()).add(1).alias('vaccination_seq')
        .over(["id_x", "vaccine_name"])
    ])


def _fix_vaccination_org(
    df: pl.DataFrame,
    replacement_org: str,
    target_vaccines: list
) -> pl.DataFrame:
    """修正特定条件下的接种机构代码"""
    return df.with_columns(
        pl.when(
            (pl.col('vaccination_org') == replacement_org) &
            (pl.col('vaccine_name').is_in(target_vaccines)) &
            (pl.col('vaccination_seq') == 1)
        )
        .then(pl.col('entry_org'))
        .otherwise(pl.col('vaccination_org'))
        .alias('vaccination_org')
    )
