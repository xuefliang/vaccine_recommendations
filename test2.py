import ibis
from ibis.expr import datatypes
from ibis import _
import ibis.selectors as s
ibis.options.interactive = True
from functools import reduce

con=ibis.duckdb.connect()
person = (
    con.read_parquet('/mnt/d/标准库接种率/data/person.parquet')
    .mutate(
        current_management_code=_.current_management_code.cast("string"),
        vaccination_org=_.vaccination_org.cast("string"),
        entry_org=_.entry_org.cast("string")
    )
    # .filter([(_.current_management_code=='334878619388') | (_.vaccination_org=='334878619388')])
    .filter(_.id_x=='216e40fef4fe4bef97244fff5c1edc91')
)

# person=person.select('id_x','birth_date','vaccination_code','vaccination_date')

# 创建疫苗代码到疫苗名称的映射
vaccine_mapping = {
    '0101': '卡介苗',
    '0201': '乙肝疫苗', '0202': '乙肝疫苗', '0203': '乙肝疫苗',
    '0301': '脊灰疫苗', '0302': '脊灰疫苗', '0303': '脊灰疫苗',
    '0304': '脊灰疫苗', '0305': '脊灰疫苗', '0306': '脊灰疫苗',
    '0311': '脊灰疫苗', '0312': '脊灰疫苗',
    '0401': '百白破疫苗', '0402': '百白破疫苗', '0403': '百白破疫苗', '4901': '百白破疫苗',
    '1201': '含麻疹成分疫苗', '1301': '含麻疹成分疫苗', '1401': '含麻疹成分疫苗',
    '5001': 'DTaPIPVHib五联疫苗',
    '0601': '白破疫苗',
    '1601': '流脑疫苗A群',
    '1701': '流脑疫苗AC群',
    '1702': '流脑替代疫苗', '1703': '流脑替代疫苗', '1704': '流脑替代疫苗', '5301': '流脑替代疫苗',
    '1801': '乙脑疫苗',
    '1802': '乙脑灭活疫苗', '1803': '乙脑灭活疫苗', '1804': '乙脑灭活疫苗',
    '1901': '甲肝疫苗',
    '1902': '甲肝灭活疫苗', '1903': '甲肝灭活疫苗'
}

case_stmt = ibis.case()
for code, name in vaccine_mapping.items():
    case_stmt = case_stmt.when(_.vaccination_code == code, ibis.literal(name))
case_stmt = case_stmt.else_(ibis.literal(None))

person = person.mutate(
    vaccine_name=case_stmt.end().cast(datatypes.String),
    current_date=ibis.date(2021,1,31)
)
# .select('id_x','birth_date','vaccine_name','vaccination_date','current_date')

person = person.mutate(
    vaccination_month=(_.vaccination_date.epoch_seconds() - _.birth_date.epoch_seconds()) / (30.44 * 24 * 3600),
    age_month=(_.current_date.epoch_seconds() - _.birth_date.epoch_seconds()) / (30.44 * 24 * 3600)
)

person = (
    person.group_by(['id_x','vaccine_name'])
    .order_by(['vaccination_date'])
    .mutate(
        dose=(_.vaccination_date.rank().cast('int64') + 1).cast('string')
    )
)


person = (
    person.mutate(
        vaccination_org=ibis.case()
        .when(
            (person.vaccination_org.isin(['777777777777', '888888888888', '999999999999'])) &
            (person.vaccine_name.isin(['乙肝疫苗', '卡介苗'])) &
            (person.dose == 1),
            person.entry_org
        )
        .else_(person.vaccination_org)
    )
    .drop('entry_org')
)

# 定筛选条件
conditions = [
    (person.vaccine_name == '卡介苗') & (person.dose == '1'),
    (person.vaccine_name == '乙肝疫苗') & person.dose.isin(['1', '2', '3']),
    (person.vaccine_name == '脊灰疫苗') & person.dose.isin(['1', '2', '3', '4']),
    (person.vaccine_name == '百白破疫苗') & person.dose.isin(['1', '2', '3', '4']),
    (person.vaccine_name == '含麻疹成分疫苗') & person.dose.isin(['1', '2']),
    (person.vaccine_name == 'DTaPIPVHib五联疫苗') & person.dose.isin(['1', '2', '3', '4']),
    (person.vaccine_name == '流脑疫苗A群') & person.dose.isin(['1', '2']),
    (person.vaccine_name == '流脑疫苗AC群') & person.dose.isin(['1', '2']),
    (person.vaccine_name == '流脑替代疫苗') & person.dose.isin(['1', '2', '3', '4']),
    (person.vaccine_name == '乙脑疫苗') & person.dose.isin(['1', '2']),
    (person.vaccine_name == '乙脑灭活疫苗') & person.dose.isin(['1', '2', '3', '4']),
    (person.vaccine_name == '甲肝疫苗') & person.dose.isin(['1']),
    (person.vaccine_name == '甲肝灭活疫苗') & person.dose.isin(['1', '2'])
]

# 合并条件
combined_condition = reduce(lambda x, y: x | y, conditions)

# 筛选数据
person = person.filter(combined_condition)

person=person.pivot_wider(names_from=['dose','vaccine_name'], values_from=['vaccination_date'],values_agg="first")

# 定义预期的列名
expected_columns = [
    '卡介苗_1', 
    '乙肝疫苗_1', '乙肝疫苗_2', '乙肝疫苗_3', 
    '脊灰疫苗_1', '脊灰疫苗_2', '脊灰疫苗_3', '脊灰疫苗_4',
    '百白破疫苗_1', '百白破疫苗_2', '百白破疫苗_3', '百白破疫苗_4',
    '含麻疹成分疫苗_1', '含麻疹成分疫苗_2',
    '乙脑疫苗_1', '乙脑疫苗_2',
    '甲肝疫苗_1',
    '流脑疫苗A群_1', '流脑疫苗A群_2',
    '流脑疫苗AC群_1', '流脑疫苗AC群_2',
    '乙脑灭活疫苗_1', '乙脑灭活疫苗_2', '乙脑灭活疫苗_3', '乙脑灭活疫苗_4',
    '甲肝灭活疫苗_1', '甲肝灭活疫苗_2',
    'DTaPIPVHib五联疫苗_1', 'DTaPIPVHib五联疫苗_2', 'DTaPIPVHib五联疫苗_3', 'DTaPIPVHib五联疫苗_4',
    '流脑替代疫苗_1', '流脑替代疫苗_2', '流脑替代疫苗_3', '流脑替代疫苗_4'
]

# 检查并添加缺失的列
for col in expected_columns:
    if col not in person.columns:
        person = person.mutate(**{col: ibis.null().cast(datatypes.date)})

# 疫苗替代
person = person.mutate(
    _脊灰疫苗_1=ibis.coalesce(_.脊灰疫苗_1, _.DTaPIPVHib五联疫苗_1),
    _脊灰疫苗_2=ibis.coalesce(_.脊灰疫苗_2, _.DTaPIPVHib五联疫苗_2),
    _脊灰疫苗_3=ibis.coalesce(_.脊灰疫苗_3, _.DTaPIPVHib五联疫苗_3),
    _脊灰疫苗_4=ibis.coalesce(_.脊灰疫苗_4, _.DTaPIPVHib五联疫苗_4),
    _流脑疫苗A群_1=ibis.coalesce(_.流脑疫苗A群_1, _.流脑替代疫苗_1),
    _流脑疫苗A群_2=(
        ibis.case()
        .when((_.vaccination_code == '5001') & (_.vaccination_month < 6),
              ibis.coalesce(_.流脑疫苗A群_2, _.流脑替代疫苗_3))
        .when((_.vaccination_code == '5001') & (_.vaccination_month >= 6),
              ibis.coalesce(_.流脑疫苗A群_2, _.流脑替代疫苗_2))
        .end()
    ),
    _流脑疫苗AC群_1=(
        ibis.case()
        .when((_.vaccination_code == '5001') & (_.vaccination_month >= 24),
              ibis.coalesce(_.流脑疫苗AC群_1, _.流脑替代疫苗_1))
        .end()
    ),
    _流脑疫苗AC群_2=(
        ibis.case()
        .when((_.vaccination_code == '5001') & (_.vaccination_month >= 60),
              ibis.coalesce(_.流脑疫苗AC群_2, _.流脑替代疫苗_2))
        .end()
    ),
    _甲肝疫苗_1=ibis.coalesce(_.甲肝疫苗_1, _.甲肝灭活疫苗_1),
    _乙脑疫苗_1=(
        ibis.case()
        .when((_.vaccination_code.isin( ['1802','1803','1804'])),
              ibis.coalesce(_.乙脑疫苗_1, _.乙脑灭活疫苗_2))
        .end()
    ),
    _乙脑疫苗_2=(
        ibis.case()
        .when((_.vaccination_code.isin(['1802','1803','1804'])) & (_.vaccination_month <= 72),
              ibis.coalesce(_.乙脑疫苗_2, _.乙脑灭活疫苗_3))
        .when((_.vaccination_code.isin(['1802','1803','1804'])) & (_.vaccination_month > 72),
              ibis.coalesce(_.乙脑疫苗_2, _.乙脑灭活疫苗_4))
        .end()
    )
)

 
person=person.mutate(
    recommended_卡介苗_1=_.birth_date,
    recommended_乙肝疫苗_1=_.birth_date,
    recommended_乙肝疫苗_2=ibis.case()
        .when(_.乙肝疫苗_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=1),
                _.乙肝疫苗_1 + ibis.interval(months=1)
            )
        ).end(),
    recommended_乙肝疫苗_3=ibis.case()
        .when(_.乙肝疫苗_2.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=6),
                ibis.case()
                    .when(_.age_month < 12,
                          ibis.least(
                              _.乙肝疫苗_1 + ibis.interval(months=6),
                              _.乙肝疫苗_2 + ibis.interval(months=1)
                          ))
                    .else_(
                        ibis.least(
                            _.乙肝疫苗_1 + ibis.interval(months=4),
                            _.乙肝疫苗_2 + ibis.interval(months=2)
                        )
                    ).end()
            )
        ).end(),
    recommended_脊灰疫苗_1=_.birth_date + ibis.interval(months=2),
    recommended_脊灰疫苗_2=ibis.case()
        .when(_.脊灰疫苗_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=3),
                _.脊灰疫苗_1 + ibis.interval(months=1)
            )
        ).end(),
    recommended_脊灰疫苗_3=ibis.case()
        .when(_.脊灰疫苗_2.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=4),
                _.脊灰疫苗_2 + ibis.interval(months=1)
            )
        ).end(),
    recommended_脊灰疫苗_4=ibis.case()
        .when(_.脊灰疫苗_3.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=4*12),
                _.脊灰疫苗_3 + ibis.interval(months=1)
            )
        ).end(),
    recommended_百白破疫苗_1=_.birth_date + ibis.interval(months=3),
    recommended_百白破疫苗_2=ibis.case()
        .when(_.百白破疫苗_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=4),
                _.百白破疫苗_1 + ibis.interval(months=1)
            )
        ).end(),
    recommended_百白破疫苗_3=ibis.case()
        .when(_.百白破疫苗_2.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=5),
                _.百白破疫苗_2 + ibis.interval(months=1)
            )
        ).end(),
    recommended_百白破疫苗_4=ibis.case()
        .when(_.百白破疫苗_3.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=18),
                _.百白破疫苗_3 + ibis.interval(months=6)
            )
        ).end(),
    recommended_含麻疹成分疫苗_1=_.birth_date + ibis.interval(months=8),
    recommended_含麻疹成分疫苗_2=ibis.case()
        .when(_.含麻疹成分疫苗_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=18),
                _.含麻疹成分疫苗_1 + ibis.interval(months=1)
            )
        ).end(),
    recommended_乙脑疫苗_1=_.birth_date + ibis.interval(months=8),
    recommended_乙脑疫苗_2=ibis.case()
        .when(_.乙脑疫苗_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=2*12),
                _.乙脑疫苗_1 + ibis.interval(months=12)
            )
        ).end(),
    recommended_甲肝疫苗_1=_.birth_date + ibis.interval(months=18),
    recommended_流脑疫苗A群_1=_.birth_date + ibis.interval(months=6),
    recommended_流脑疫苗A群_2=ibis.case()
        .when(_.流脑疫苗A群_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=9),
                _.流脑疫苗A群_1 + ibis.interval(months=3)
            )
        ).end(),
    recommended_流脑疫苗AC群_1=ibis.case()
        .when(
            (_.age_month >= 3*12) & (_.流脑疫苗A群_1.notnull()) & (_.流脑疫苗A群_2.notnull()),
            _.birth_date + ibis.interval(months=3*12)
        )
        .when(
            (_.age_month >= 2*12) & (_.流脑疫苗A群_1.notnull()) & (_.流脑疫苗A群_2.isnull()),
            _.流脑疫苗A群_1 + ibis.interval(months=3)
        )
        .when(
            (_.age_month >= 2*12) & (_.流脑疫苗A群_1.isnull()) & (_.流脑疫苗A群_2.isnull()),
            _.birth_date + ibis.interval(months=2*12)
        )
        .end(), 
    recommended_流脑疫苗AC群_2=ibis.case()
        .when(_.流脑疫苗AC群_1.isnull(), ibis.null())
        .else_(
            ibis.greatest(
                _.birth_date + ibis.interval(months=5*12),
                _.流脑疫苗AC群_1 + ibis.interval(months=3*12)
            )
        ).end()
)


vaccine_names = [
    '卡介苗_1', '乙肝疫苗_1', '乙肝疫苗_2', '乙肝疫苗_3', '脊灰疫苗_1', '脊灰疫苗_2', '脊灰疫苗_3', '脊灰疫苗_4',
    '百白破疫苗_1', '百白破疫苗_2', '百白破疫苗_3', '百白破疫苗_4', '含麻疹成分疫苗_1', '含麻疹成分疫苗_2',
    '乙脑疫苗_1', '乙脑疫苗_2', '甲肝疫苗_1', '流脑疫苗A群_1', '流脑疫苗A群_2', '流脑疫苗AC群_1', '流脑疫苗AC群_2'
]

# 创建一个包含疫苗名称的表
vaccine_table = ibis.memtable({'vaccination_name': vaccine_names})

# 使用 cross_join 将 person 表与 vaccine_table 连接
info = (
    person.select('id_x', 'patient_name', 'gender_code', 'birth_date', 'current_management_code', 'age_month')
    .group_by(['id_x', 'patient_name', 'gender_code', 'birth_date', 'current_management_code', 'age_month'])
    .agg()
    .cross_join(vaccine_table)
)

vaccination=person.select('id_x','卡介苗_1','乙肝疫苗_1','乙肝疫苗_2','乙肝疫苗_3','脊灰疫苗_1','脊灰疫苗_2','脊灰疫苗_3','脊灰疫苗_4',
'百白破疫苗_1','百白破疫苗_2','百白破疫苗_3','百白破疫苗_4','含麻疹成分疫苗_1','含麻疹成分疫苗_2','乙脑疫苗_1','乙脑疫苗_2','甲肝疫苗_1','流脑疫苗A群_1','流脑疫苗A群_2','流脑疫苗AC群_1','流脑疫苗AC群_2')

vaccination = (vaccination.pivot_longer(s.contains(['卡介苗_1', '乙肝疫苗_1', '乙肝疫苗_2', '乙肝疫苗_3', 
    '脊灰疫苗_1', '脊灰疫苗_2', '脊灰疫苗_3', '脊灰疫苗_4',
    '百白破疫苗_1', '百白破疫苗_2', '百白破疫苗_3', '百白破疫苗_4', 
    '含麻疹成分疫苗_1', '含麻疹成分疫苗_2', '乙脑疫苗_1', '乙脑疫苗_2',
    '甲肝疫苗_1', '流脑疫苗A群_1', '流脑疫苗A群_2', 
    '流脑疫苗AC群_1', '流脑疫苗AC群_2']),
    values_to="vaccination_date",
    names_to="vaccination_name",)
    .group_by(['id_x', 'vaccination_name', 'vaccination_date'])
    .agg()
    .filter(_.vaccination_date.notnull())
)

recommended=person.select('id_x','recommended_卡介苗_1','recommended_乙肝疫苗_1','recommended_乙肝疫苗_2','recommended_乙肝疫苗_3','recommended_脊灰疫苗_1','recommended_脊灰疫苗_2','recommended_脊灰疫苗_3','recommended_脊灰疫苗_4',
'recommended_百白破疫苗_1','recommended_百白破疫苗_2','recommended_百白破疫苗_3','recommended_百白破疫苗_4','recommended_含麻疹成分疫苗_1','recommended_含麻疹成分疫苗_2','recommended_乙脑疫苗_1','recommended_乙脑疫苗_2','recommended_甲肝疫苗_1','recommended_流脑疫苗A群_1','recommended_流脑疫苗A群_2','recommended_流脑疫苗AC群_1','recommended_流脑疫苗AC群_2')

recommended = (recommended.pivot_longer(s.contains(['recommended_']),
    values_to="recommended_date",
    names_to="recommended_name")
    .group_by(['id_x', 'recommended_name', 'recommended_date'])
    .agg()
    .filter(_.recommended_date.notnull())
    .mutate(recommended_name=_.recommended_name.re_replace('recommended_', ''))
)


result = (
    info.left_join(
        vaccination,
        [info.id_x == vaccination.id_x, 
         info.vaccination_name == vaccination.vaccination_name]
    )
    .drop(['id_x_right', 'vaccination_name_right'])
    .left_join(
        recommended,
        [_.id_x == recommended.id_x,
         _.vaccination_name == recommended.recommended_name]
    )
    .drop(['id_x_right', 'recommended_name'])
)