import polars as pl

def is_valid_id_card():
    return (
        pl.col("feld1").is_not_null() &
        (pl.col("feld1").str.len_chars() == 18) & 
        (pl.col("feld1").str.slice(0, 17).str.contains(r"^\d{17}$")) &
        (pl.col("feld1").str.slice(17, 1).str.contains(r"^[\dXx]$"))
    )

sw=(pl.read_excel('/mnt/c/Users/Administrator/Downloads/副本疾控平台从国家库中对接的死亡数据.xlsx')
    .select(["Id_Card","Death_Date"])
    .with_columns(pl.col.Death_Date.str.split('T').list[0].str.to_date("%Y%m%d"))
)


(
    pl.read_csv('/mnt/c/Users/Administrator/Downloads/tmp_gmxzd_20250929.csv', 
                schema_overrides={'feld10': pl.Utf8,'feld2':pl.Utf8,'feld11':pl.Utf8,'feld5':pl.Utf8})
    .join(sw, left_on='feld1', right_on='Id_Card', how="left")
    .with_columns(is_valid_id_card().alias("is_valid_id"))
        .with_columns(
        pl.when(pl.col.is_valid_id)
        .then(
            pl.col("feld1").str.slice(6, 8).str.to_date(format="%Y%m%d", strict=False)
        )
        .otherwise(None)
        .alias("feld5"))
    .with_columns(
        pl.when(pl.col.is_valid_id)
        .then(
            pl.when(
                pl.col("feld4").is_not_null() &
                pl.col("feld1").str.slice(16, 1).is_not_null() &
                (pl.col("feld1").str.slice(16, 1).str.to_integer(base=10, strict=False) % 2 == 1)
            )
            .then(pl.lit("男"))
            .otherwise(pl.lit("女"))
        )
        .otherwise(None)
        .alias("feld4")
    )
    .with_columns(
        pl.when(pl.col('Death_Date').is_not_null())
        .then(pl.lit('是'))
        .otherwise(None)
        .alias("Death")
    )
    .drop(pl.col('is_valid_id'))
    .to_pandas()
    .to_excel('/mnt/c/Users/Administrator/Downloads/tmp_gmxzd_20250929.xlsx',index=False)
)


df=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/inoc_ym_sjjzs_v3.xlsx',read_options={'header_row':1})
    .filter( (pl.col.生产企业=='兰州生物') & (pl.col.疫苗属性=='非免疫规划疫苗'))
    .group_by(["生产企业","疫苗名称"])
    .agg(pl.col.接种数.sum().alias("接种数"))
    .with_columns(pl.lit('2023').alias("年份"))
)

df1=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/inoc_ym_sjjzs_v3 (1).xlsx',read_options={'header_row':1})
    .filter( (pl.col.生产企业=='兰州生物') & (pl.col.疫苗属性=='非免疫规划疫苗'))
    .group_by(["生产企业","疫苗名称"])
    .agg(pl.col.接种数.sum().alias("接种数"))
    .with_columns(pl.lit('2024').alias("年份"))
)

df2=(
    pl.read_excel('/mnt/c/Users/Administrator/Downloads/inoc_ym_sjjzs_v3 (2).xlsx',read_options={'header_row':1})
    .filter( (pl.col.生产企业=='兰州生物') & (pl.col.疫苗属性=='非免疫规划疫苗'))
    .group_by(["生产企业","疫苗名称"])
    .agg(pl.col.接种数.sum().alias("接种数"))
    .with_columns(pl.lit('2025').alias("年份"))
)

(
    pl.concat([df,df1,df2])
    .to_pandas()
    .to_excel('/mnt/c/Users/Administrator/Downloads/兰州生物.xlsx')
)