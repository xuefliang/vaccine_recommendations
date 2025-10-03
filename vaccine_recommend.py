import polars as pl

def calculate_vaccine_recommendations(person_df: pl.DataFrame) -> pl.DataFrame:
    """
    计算所有疫苗的推荐时间
    
    Args:
        person_df: 包含疫苗接种记录的DataFrame
        
    Returns:
        pl.DataFrame: 包含所有疫苗推荐时间的DataFrame
    """
    
    # 定义疫苗推荐配置
    vaccine_configs = [
        # 卡介苗
        {
            'name': 'BCG_1',
            'vaccine_name': '卡介苗',
            'vaccine_code': '0101',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '1d',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '卡介苗') | (df.get_column('age') > 4))
        },
        
        # 乙肝疫苗第1针
        {
            'name': 'HBV_1',
            'vaccine_name': '乙肝疫苗',
            'vaccine_code': '0201',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '1d',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '乙肝疫苗') & (df.get_column('age') > 6))
        },
        
        # 乙肝疫苗第2针
        {
            'name': 'HBV_2',
            'vaccine_name': '乙肝疫苗',
            'vaccine_code': '0201',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '乙肝疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('1mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '乙肝疫苗') & ((df.get_column('age_month') < 1) | (df.get_column('age') > 6)))
        },
        
        # 乙肝疫苗第3针（复杂逻辑）
        {
            'name': 'HBV_3',
            'vaccine_name': '乙肝疫苗',
            'vaccine_code': '0201',
            'dose': 3,
            'base_time': 'complex',
            'complex_calc': True
        },
        
        # 乙肝疫苗第4针
        {
            'name': 'HBV_4',
            'vaccine_name': '乙肝疫苗',
            'vaccine_code': '0201',
            'dose': 4,
            'base_time': 'dose_based',
            'condition': lambda df: (
                ((df.get_column('hepatitis_mothers') == '1') |
                 ((df.get_column('hepatitis_mothers') == '3') & (df.get_column('birth_weight') < 2000))) &
                (df.get_column('vaccination_seq') == 3) &
                (df.get_column('vaccine_name') == '乙肝疫苗')
            ),
            'time_calc': lambda df: df.get_column('vaccination_date').dt.offset_by('5mo')
        },
        
        # 脊灰疫苗
        {
            'name': 'POL_1',
            'vaccine_name': '脊灰疫苗',
            'vaccine_code': '0303',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '2mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '脊灰疫苗') & ((df.get_column('age_month') < 2) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'POL_2',
            'vaccine_name': '脊灰疫苗',
            'vaccine_code': '0303',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '脊灰疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('3mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '脊灰疫苗') & ((df.get_column('age_month') < 3) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'POL_3',
            'vaccine_name': '脊灰疫苗',
            'vaccine_code': '0311',
            'dose': 3,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 2) & (df.get_column('vaccine_name') == '脊灰疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('4mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '脊灰疫苗') & ((df.get_column('age_month') < 4) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'POL_4',
            'vaccine_name': '脊灰疫苗',
            'vaccine_code': '0311',
            'dose': 4,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 3) & (df.get_column('vaccine_name') == '脊灰疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('4y'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '脊灰疫苗') & ((df.get_column('age') < 4) | (df.get_column('age') > 6)))
        },
        
        # 百白破疫苗
        {
            'name': 'DPT_1',
            'vaccine_name': '百白破疫苗',
            'vaccine_code': '0402',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '2mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '百白破疫苗') & ((df.get_column('age_month') < 2) | (df.get_column('age') > 7)))
        },
        
        {
            'name': 'DPT_2',
            'vaccine_name': '百白破疫苗',
            'vaccine_code': '0402',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '百白破疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('4mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '百白破疫苗') & ((df.get_column('age_month') < 4) | (df.get_column('age') > 7)))
        },
        
        {
            'name': 'DPT_3',
            'vaccine_name': '百白破疫苗',
            'vaccine_code': '0402',
            'dose': 3,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 2) & (df.get_column('vaccine_name') == '百白破疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('6mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '百白破疫苗') & ((df.get_column('age_month') < 6) | (df.get_column('age') > 7)))
        },
        
        {
            'name': 'DPT_4',
            'vaccine_name': '百白破疫苗',
            'vaccine_code': '0402',
            'dose': 4,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 3) & (df.get_column('vaccine_name') == '百白破疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('18mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '百白破疫苗') & ((df.get_column('age_month') < 18) | (df.get_column('age') > 7)))
        },
        
        {
            'name': 'DPT_5',
            'vaccine_name': '百白破疫苗',
            'vaccine_code': '0402',
            'dose': 5,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 4) & (df.get_column('vaccine_name') == '百白破疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('6y'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '百白破疫苗') & ((df.get_column('age') < 6) | (df.get_column('age') > 7)))
        },
        
        # 白破疫苗
        {
            'name': 'DT_1',
            'vaccine_name': '白破疫苗',
            'vaccine_code': '0601',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '7y',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '白破疫苗') & ((df.get_column('age') < 7) | (df.get_column('age') > 11)))
        },
        
        # 含麻疹成分疫苗
        {
            'name': 'MCV_1',
            'vaccine_name': '含麻疹成分疫苗',
            'vaccine_code': '1201',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '8mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '含麻疹成分疫苗') & ((df.get_column('age_month') < 8) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'MCV_2',
            'vaccine_name': '含麻疹成分疫苗',
            'vaccine_code': '1201',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '含麻疹成分疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('18mo'),
                df.get_column('vaccination_date').dt.offset_by('1mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '含麻疹成分疫苗') & ((df.get_column('age_month') < 18) | (df.get_column('age') > 7)))
        },
        
        # A群流脑疫苗
        {
            'name': 'MAV_1',
            'vaccine_name': 'A群流脑疫苗',
            'vaccine_code': '1601',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '6mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == 'A群流脑疫苗') & ((df.get_column('age_month') < 6) | (df.get_column('age_month') > 23)))
        },
        
        {
            'name': 'MAV_2',
            'vaccine_name': 'A群流脑疫苗',
            'vaccine_code': '1601',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: ((df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == 'A群流脑疫苗')) | ((df.get_column('vaccination_seq') == 1) & df.get_column('vaccination_code').is_in(['1702', '1703', '1704'])),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('9mo'),
                df.get_column('vaccination_date').dt.offset_by('3mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == 'A群流脑疫苗') & ((df.get_column('age_month') < 9) | (df.get_column('age_month') > 23)))
        },
        
        # A群C群流脑疫苗（复杂逻辑）
        {
            'name': 'MAC_1',
            'vaccine_name': 'A群C群流脑疫苗',
            'vaccine_code': '1701',
            'dose': 1,
            'base_time': 'complex',
            'complex_calc': True
        },
        
        {
            'name': 'MAC_2',
            'vaccine_name': 'A群C群流脑疫苗',
            'vaccine_code': '1701',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: ((df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == 'A群C群流脑疫苗')) | ((df.get_column('vaccination_seq') == 1) & (df.get_column('vacc_month') >= 24) & df.get_column('vaccination_code').is_in(['1702', '1703', '1704'])),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('5y'),
                df.get_column('vaccination_date').dt.offset_by('3y')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == 'A群C群流脑疫苗') & (df.get_column('age') < 5))
        },
        
        # 乙脑疫苗
        {
            'name': 'JE_1',
            'vaccine_name': '乙脑疫苗',
            'vaccine_code': '1801',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '8mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '乙脑疫苗') & ((df.get_column('age_month') < 8) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'JE_2',
            'vaccine_name': '乙脑疫苗',
            'vaccine_code': '1801',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '乙脑疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('2y'),
                df.get_column('vaccination_date').dt.offset_by('12mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '乙脑疫苗') & ((df.get_column('age') < 2) | (df.get_column('age') > 6)))
        },
        
        # 甲肝疫苗
        {
            'name': 'HAV_1',
            'vaccine_name': '甲肝疫苗',
            'vaccine_code': '1901',
            'dose': 1,
            'base_time': 'birth_date',
            'offset': '18mo',
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '甲肝疫苗') & ((df.get_column('age_month') < 18) | (df.get_column('age') > 6)))
        },
        
        {
            'name': 'HAVL_2',
            'vaccine_name': '甲肝疫苗',
            'vaccine_code': '1903',
            'dose': 2,
            'base_time': 'dose_based',
            'condition': lambda df: (df.get_column('vaccination_seq') == 1) & (df.get_column('vaccine_name') == '甲肝疫苗'),
            'time_calc': lambda df: pl.max_horizontal([
                df.get_column('birth_date').dt.offset_by('2y'),
                df.get_column('vaccination_date').dt.offset_by('6mo')
            ]),
            'age_filter': lambda df: ~((df.get_column('vaccine_name') == '甲肝疫苗') & ((df.get_column('age_month') < 24) | (df.get_column('age') > 6)))
        }
    ]
    
    def calculate_single_vaccine(df, config):
        """计算单个疫苗的推荐时间"""
        
        # 处理复杂逻辑的疫苗
        if config.get('complex_calc'):
            if config['name'] == 'HBV_3':
                return calculate_hbv_3(df)
            elif config['name'] == 'MAC_1':
                return calculate_mac_1(df)
        
        # 基于出生日期的推荐时间
        if config['base_time'] == 'birth_date':
            df_calc = df.with_columns([
                pl.col("birth_date").dt.offset_by(config['offset']).alias("recommended_dates"),
                pl.lit(config['vaccine_code']).alias("recommended_vacc"),
                pl.lit(config['dose']).alias('recommended_seq')
            ])
        
        # 基于前一剂次的推荐时间
        elif config['base_time'] == 'dose_based':
            df_calc = df.with_columns([
                pl.when(config['condition'](df))
                .then(config['time_calc'](df))
                .otherwise(None)
                .alias("recommended_dates"),
                pl.lit(config['vaccine_code']).alias("recommended_vacc"),
                pl.lit(config['dose']).alias('recommended_seq')
            ])
        
        # 应用年龄过滤条件
        if 'age_filter' in config:
            df_calc = df_calc.with_columns(
                recommended_dates=pl.when(config['age_filter'](df_calc))
                .then(None)
                .otherwise(pl.col("recommended_dates"))
            )
        
        # 按个人分组聚合
        return df_calc.group_by("id_x").agg([
            pl.col("recommended_dates").drop_nulls().first(),
            pl.col("recommended_vacc").first(),
            pl.col("recommended_seq").first(),
            pl.col("birth_date").first(),
            pl.col("age").first(),
            pl.col("entry_org").first(),
            pl.col("entry_date").first(),
            pl.col("current_management_code").first()
        ])
    
    def calculate_hbv_3(df):
        """乙肝疫苗第3针的复杂计算逻辑"""
        return (
            df.with_columns([
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
                    (pl.col("vaccination_seq") == 2) & 
                    (pl.col("vaccine_name") == '乙肝疫苗') & 
                    (pl.col("age") < 1)
                )
                .then(pl.col("vaccination_date").dt.offset_by("1mo"))
                .when(
                    (pl.col("vaccination_seq") == 2) & 
                    (pl.col("vaccine_name") == '乙肝疫苗') & 
                    (pl.col("age") >= 1)
                )
                .then(pl.col("vaccination_date").dt.offset_by("60d"))
                .otherwise(None)
                .alias("from_dose2"),
                
                pl.lit('0201').alias("recommended_vacc"),
                pl.lit(3).alias('recommended_seq')
            ])
            .with_columns(
                recommended_dates=pl.when(
                    (pl.col("vaccine_name") == '乙肝疫苗') & 
                    ((pl.col('age_month') < 6) | (pl.col('age') > 6))
                )
                .then(None)
                .otherwise(pl.max_horizontal([
                    pl.col("from_dose1"),
                    pl.col("from_dose2")
                ]))
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
    
    def calculate_mac_1(df):
        """A群C群流脑疫苗第1针的复杂计算逻辑"""
        return (
            df.with_columns([
                # 标记A群流脑疫苗免疫剂次数
                ((pl.col("vaccine_name") == 'A群流脑疫苗') & 
                 (pl.col("age") >= 2)).sum().over("id_x").alias("his_ma"),
            ])
            .with_columns([
                pl.when((pl.col("vaccination_seq") == 2) & 
                       (pl.col("vaccine_name") == 'A群流脑疫苗'))
                .then(pl.col("birth_date").dt.offset_by("3y"))
                .otherwise(None)
                .alias("recommended_dates"),
                pl.lit('1701').alias("recommended_vacc"),
                pl.lit(1).alias('recommended_seq')
            ])
            .with_columns(
                recommended_dates=pl.when(
                    (pl.col("vaccine_name") == 'A群C群流脑疫苗') & 
                    ((pl.col("age") < 3) | (pl.col("age") > 6))
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
    
    # 计算所有疫苗推荐时间
    all_recommendations = []
    
    for config in vaccine_configs:
        try:
            recommendation = calculate_single_vaccine(person_df, config)
            if recommendation.height > 0:  # 如果有推荐数据
                all_recommendations.append(recommendation)
        except Exception as e:
            print(f"计算 {config['name']} 时发生错误: {e}")
            continue
    
    # 合并所有推荐
    if all_recommendations:
        final_recommendations = pl.concat(all_recommendations, how="vertical")
        return final_recommendations.sort(["id_x", "recommended_vacc", "recommended_seq"])
    else:
        return pl.DataFrame()

# 使用示例
vaccine_tbl = pl.read_excel('ym_bm.xlsx')

# 准备person数据（您的原始代码）
person = (
    pl.read_csv('/mnt/d/标准库接种率/data/person2.csv', 
                schema_overrides={"vaccination_code": pl.Utf8, "birth_date": pl.Datetime, 
                                "vaccination_date": pl.Datetime, "hepatitis_mothers": pl.Utf8})
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
            (pl.col("vaccination_date").dt.month() - pl.col("birth_date").dt.month()) + 
            pl.when(pl.col("vaccination_date").dt.day() >= pl.col("birth_date").dt.day())
            .then(0)
            .otherwise(-1)
        ).alias('vacc_month')])
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

# 调用函数计算所有疫苗推荐时间
vaccine_recommendations = calculate_vaccine_recommendations(person)

