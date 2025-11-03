import polars as pl

vaccine_tbl=pl.read_excel('ym_bm.xlsx')

cutoff_date='2021-01-15'

# 计算年龄和接种月龄，接种时间小于截止时间，剂次重排,五联疫苗及甲乙肝疫苗拆分
person=(
    pl.read_csv('/mnt/d/标准库接种率/data/person2.csv',schema_overrides={"vaccination_code":pl.Utf8,"birth_date":pl.Datetime,"vaccination_date":pl.Datetime,"hepatitis_mothers":pl.Utf8})
    .with_columns([
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias('expiration_date'),
        pl.lit(cutoff_date).str.to_date().dt.month_start().alias('mon_start'),
        pl.lit(cutoff_date).str.to_date().dt.month_end().alias('mon_end')
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
    .sort(["id_x", "vaccine_name", "vaccination_date"])
    .with_columns([
        pl.int_range(pl.len()).add(1).alias('vaccination_seq').over(["id_x", "vaccine_name"])
    ])
    .with_columns(
        pl.when((pl.col('vaccination_org') == 777777777777) & (pl.col('vaccine_name').is_in(['乙肝疫苗','卡介苗'])) & (pl.col('vaccination_seq')==1))
        .then(pl.col('entry_org'))
        .otherwise(pl.col('vaccination_org'))
        .alias('vaccination_org')
    )
)
