import ibis
from ibis.expr import datatypes
from ibis import _
import ibis.selectors as s
from functools import reduce
from datetime import date

class VaccinationAnalyzer:
    def __init__(self, person, current_date):
        ibis.options.interactive = True
        self.person = person
        self.current_date = current_date
        self.vaccine_mapping = {
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
        self.expected_columns = [
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
        self.vaccine_names = [
            '卡介苗_1', '乙肝疫苗_1', '乙肝疫苗_2', '乙肝疫苗_3', '脊灰疫苗_1', '脊灰疫苗_2', '脊灰疫苗_3', '脊灰疫苗_4',
            '百白破疫苗_1', '百白破疫苗_2', '百白破疫苗_3', '百白破疫苗_4', '含麻疹成分疫苗_1', '含麻疹成分疫苗_2',
            '乙脑疫苗_1', '乙脑疫苗_2', '甲肝疫苗_1', '流脑疫苗A群_1', '流脑疫苗A群_2', '流脑疫苗AC群_1', '流脑疫苗AC群_2'
        ]

    def preprocess_data(self):
        case_stmt = ibis.case()
        for code, name in self.vaccine_mapping.items():
            case_stmt = case_stmt.when(_.vaccination_code == code, ibis.literal(name))
        case_stmt = case_stmt.else_(ibis.literal(None))

        self.person = self.person.mutate(
            vaccine_name=case_stmt.end().cast(datatypes.String),
            current_date=ibis.date(self.current_date.year, self.current_date.month, self.current_date.day)
        )

        self.person = self.person.mutate(
            vaccination_month=(_.vaccination_date.epoch_seconds() - _.birth_date.epoch_seconds()) / (30.44 * 24 * 3600),
            age_month=(_.current_date.epoch_seconds() - _.birth_date.epoch_seconds()) / (30.44 * 24 * 3600)
        )

        self.person = (
            self.person.group_by(['id_x','vaccine_name'])
            .order_by(['vaccination_date'])
            .mutate(
                dose=(_.vaccination_date.rank().cast('int64') + 1).cast('string')
            )
        )

        self.person = (
            self.person.mutate(
                vaccination_org=ibis.case()
                .when(
                    (_.vaccination_org.isin(['777777777777', '888888888888', '999999999999'])) &
                    (_.vaccine_name.isin(['乙肝疫苗', '卡介苗'])) &
                    (_.dose == '1'),
                    _.entry_org
                )
                .else_(_.vaccination_org)
                .end()
                .cast('string') 
            )
            .drop('entry_org')
        )

        # 定义筛选条件
        conditions = [
            (self.person.vaccine_name == '卡介苗') & (self.person.dose == '1'),
            (self.person.vaccine_name == '乙肝疫苗') & self.person.dose.isin(['1', '2', '3']),
            (self.person.vaccine_name == '脊灰疫苗') & self.person.dose.isin(['1', '2', '3', '4']),
            (self.person.vaccine_name == '百白破疫苗') & self.person.dose.isin(['1', '2', '3', '4']),
            (self.person.vaccine_name == '含麻疹成分疫苗') & self.person.dose.isin(['1', '2']),
            (self.person.vaccine_name == 'DTaPIPVHib五联疫苗') & self.person.dose.isin(['1', '2', '3', '4']),
            (self.person.vaccine_name == '流脑疫苗A群') & self.person.dose.isin(['1', '2']),
            (self.person.vaccine_name == '流脑疫苗AC群') & self.person.dose.isin(['1', '2']),
            (self.person.vaccine_name == '流脑替代疫苗') & self.person.dose.isin(['1', '2', '3', '4']),
            (self.person.vaccine_name == '乙脑疫苗') & self.person.dose.isin(['1', '2']),
            (self.person.vaccine_name == '乙脑灭活疫苗') & self.person.dose.isin(['1', '2', '3', '4']),
            (self.person.vaccine_name == '甲肝疫苗') & self.person.dose.isin(['1']),
            (self.person.vaccine_name == '甲肝灭活疫苗') & self.person.dose.isin(['1', '2'])
        ]

        # 合并条件
        combined_condition = reduce(lambda x, y: x | y, conditions)

        # 筛选数据
        self.person = self.person.filter(combined_condition)

        self.vaccination_org = self.person.select(['id_x', 'vaccine_name', 'dose', 'vaccination_org']).mutate(vaccine_name=_.vaccine_name + '_' + _.dose).drop('dose')


    def pivot_data(self):
        self.person = self.person.pivot_wider(names_from=['dose','vaccine_name'], values_from=['vaccination_date'], values_agg="first")

        # 检查并添加缺失的列
        for col in self.expected_columns:
            if col not in self.person.columns:
                self.person = self.person.mutate(**{col: ibis.null().cast(datatypes.date)})

    def substitute_vaccines(self):
        self.person = self.person.mutate(
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
                .when((_.vaccination_code.isin(['1802','1803','1804'])),
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

    def calculate_recommended_dates(self):
        self.person = self.person.mutate(
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

    def prepare_result(self):
        # 获取疫苗接种机构信息
        vaccination_org = self.vaccination_org

        # 创建一个包含疫苗名称的表
        vaccine_table = ibis.memtable({'vaccination_name': self.vaccine_names})

        # 使用 cross_join 将 person 表与 vaccine_table 连接
        self.info = (
            self.person.select('id_x', 'patient_name', 'gender_code', 'birth_date', 'current_management_code', 'age_month')
            .group_by(['id_x', 'patient_name', 'gender_code', 'birth_date', 'current_management_code', 'age_month'])
            .agg()
            .cross_join(vaccine_table)
        )

        self.vaccination = self.person.select('id_x', *self.vaccine_names)

        self.vaccination = (self.vaccination.pivot_longer(s.contains(self.vaccine_names),
            values_to="vaccination_date",
            names_to="vaccination_name",)
            .group_by(['id_x', 'vaccination_name', 'vaccination_date'])
            .agg()
            .filter(_.vaccination_date.notnull())
        )

        recommended_columns = ['recommended_' + name for name in self.vaccine_names]
        self.recommended = self.person.select('id_x', *recommended_columns)

        self.recommended = (self.recommended.pivot_longer(s.contains(['recommended_']),
            values_to="recommended_date",
            names_to="recommended_name")
            .group_by(['id_x', 'recommended_name', 'recommended_date'])
            .agg()
            .filter(_.recommended_date.notnull())
            .mutate(recommended_name=_.recommended_name.re_replace('recommended_', ''))
        )

        self.result = (
            self.info.left_join(
                self.vaccination,
                [self.info.id_x == self.vaccination.id_x, 
                 self.info.vaccination_name == self.vaccination.vaccination_name]
            )
            .drop(['id_x_right', 'vaccination_name_right'])
            .left_join(
                self.recommended,
                [_.id_x == self.recommended.id_x,
                 _.vaccination_name == self.recommended.recommended_name]
            )
            .drop(['id_x_right', 'recommended_name'])
            .left_join(  # 添加疫苗接种机构信息
                vaccination_org,
                [_.id_x == vaccination_org.id_x,
                 _.vaccination_name == vaccination_org.vaccine_name]
            )
            .drop(['id_x_right', 'vaccine_name'])
        )

    def analyze(self):
        self.preprocess_data()
        self.pivot_data()
        self.substitute_vaccines()
        self.calculate_recommended_dates()
        self.prepare_result()
        return self.result

# 使用示例
current_date = date(2021, 1, 31)

con = ibis.duckdb.connect()
person = (
    con.read_parquet('/mnt/d/标准库接种率/data/person.parquet')
    .mutate(
        current_management_code=_.current_management_code.cast("string"),
        vaccination_org=_.vaccination_org.cast("string"),
        entry_org=_.entry_org.cast("string")
    )
    .filter([_.current_management_code=='334878619388'])
)
analyzer = VaccinationAnalyzer(person, current_date)
result = analyzer.analyze()

result_df=result.to_pandas()

# 定义不同疫苗的计算条件
vaccine_conditions = [
    {
        'vaccines': ['卡介苗'],
        'actual_min_age': 0, 'actual_max_age': 4*12,
        'expected_min_age': 0, 'expected_max_age': 4*12,
        'doses': [1]
    },
    {
        'vaccines': ['乙肝疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 0, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['乙肝疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 1, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['乙肝疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 6, 'expected_max_age': 6*12,
        'doses': [3]
    },
    {
        'vaccines': ['脊灰疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 2, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['脊灰疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 3, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['脊灰疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 4, 'expected_max_age': 6*12,
        'doses': [3]
    },
    {
        'vaccines': ['脊灰疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 4*12, 'expected_max_age': 6*12,
        'doses': [4]
    },
    {
        'vaccines': ['百白破疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 3, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['百白破疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 4, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['百白破疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 5, 'expected_max_age': 6*12,
        'doses': [3]
    },
    {
        'vaccines': ['百白破疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 18, 'expected_max_age': 6*12,
        'doses': [4]
    },
    {
        'vaccines': ['含麻疹成分疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 8, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['含麻疹成分疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 18, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['乙脑疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 8, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['乙脑疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 2*12, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['甲肝疫苗'],
        'actual_min_age': 0, 'actual_max_age': 18*12,
        'expected_min_age': 18, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['流脑疫苗A群'],
        'actual_min_age': 0, 'actual_max_age': 2*12,
        'expected_min_age': 6, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['流脑疫苗A群'],
        'actual_min_age': 0, 'actual_max_age': 2*12,
        'expected_min_age': 9, 'expected_max_age': 6*12,
        'doses': [2]
    },
    {
        'vaccines': ['流脑疫苗AC群'],
        'actual_min_age': 2*12, 'actual_max_age': 18*12,
        'expected_min_age': 2*12, 'expected_max_age': 6*12,
        'doses': [1]
    },
    {
        'vaccines': ['流脑疫苗AC群'],
        'actual_min_age': 5*12, 'actual_max_age': 18*12,
        'expected_min_age': 5*12, 'expected_max_age': 18*12,
        'doses': [2]
    }
]

current_date = date(2021, 1, 31)

for condition in vaccine_conditions:
    recommendations=result
    vaccine_patterns = [f"{vaccine}_{dose}" for vaccine in condition['vaccines'] for dose in condition['doses']]
    # 计算实种数
    mask_actual = (
        (recommendations.vaccination_date.year() == current_date.year) &
        (recommendations.vaccination_date.month() == current_date.month) &
        (recommendations.age >= condition['actual_min_age']) &
        (recommendations.age < condition['actual_max_age']) &
        (recommendations.vaccination_name.isin(vaccine_patterns)) &
        (recommendations.vaccination_date.notnull()) &
        (recommendations.vaccination_org == recommendations.current_management_code)
    )
        
    actual = (
        recommendations.filter(mask_actual)
        .group_by(['vaccination_name', 'vaccination_org'])
        .agg(actual_count=_.id_x.nunique())
    )

    test=actual.to_pandas()
    
    # 计算应种数
    mask_expected = (
        ((recommendations.recommended_date.year() * 12 + recommendations.recommended_date.month()) >= 
         ((current_date - relativedelta(months=12)).year * 12 + (current_date - relativedelta(months=12)).month)) &
        ((recommendations.recommended_date.year() * 12 + recommendations.recommended_date.month()) <= 
         (current_date.year * 12 + current_date.month)) &
        (recommendations.age >= condition['expected_min_age']) & 
        (recommendations.age < condition['expected_max_age']) &
        (recommendations.vaccination_name.isin(vaccine_patterns))
    )
    
    expected = (
        recommendations.filter(mask_expected)
        .group_by(['vaccination_name', 'current_management_code'])
        .agg(expected_count=_.id_x.nunique())
    )

    test2=expected.to_pandas()


    result=(
        expected
        .left_join(actual,
        [expected.vaccination_name == actual.vaccination_name,
        expected.current_management_code == actual.vaccination_org])
        .mutate(prop=(_.actual_count / (_.expected_count + _.actual_count) * 100).cast(dt.Decimal(10, 2)))
    )

    test3=result.to_pandas()