import ibis
from ibis.expr import datatypes
from ibis import _
import ibis.selectors as s
from functools import reduce
from datetime import date
import ibis.expr.datatypes as dt
import pandas as pd
from dateutil.relativedelta import relativedelta

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
            '流脑替代疫苗_1', '流脑替代疫苗_2', '流脑替代苗_3', '流脑替代疫苗_4'
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
            (self.person.vaccine_name == 'DTaPIPVHib五联疫') & self.person.dose.isin(['1', '2', '3', '4']),
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

    def update_recommended_dates(self):
        self.result = self.result.mutate(
            recommended_date=ibis.case()
                .when(
                    (self.result.vaccination_date <= self.result.recommended_date) |
                    (self.result.vaccination_date < ibis.date(self.current_date.year, self.current_date.month, self.current_date.day)),
                    ibis.null()
                )
                .else_(self.result.recommended_date)
            .end()
        )

    def analyze(self):
        self.preprocess_data()
        self.pivot_data()
        self.substitute_vaccines()
        self.calculate_recommended_dates()
        self.prepare_result()
        self.update_recommended_dates()
        return self.result


    def cohort_rate(self, by_age=False):
        
        recommendations = self.analyze().mutate(age=_.age_month // 12)

        if by_age:
            expected = (
                recommendations
                .filter(_.age >= 1)
                .group_by(['current_management_code', 'age'])
                .agg(cnt=_.id_x.nunique())
            )

            actual = (
                recommendations
                .filter((_.age >= 1) & _.vaccination_date.notnull())
                .group_by(['current_management_code', 'age', 'vaccination_name'])
                .agg(vac=_.id_x.nunique())
            )

            result = (
                actual.join(expected, ['current_management_code', 'age'])
                .mutate(percent=(_.vac / _.cnt * 100).cast(dt.Decimal(10, 2)))
            )

        else:
            expected = (
                recommendations
                .filter(_.age >= 1)
                .group_by('current_management_code')
                .agg(cnt=_.id_x.nunique())
            )

            actual = (
                recommendations
                .filter((_.age >= 1) & _.vaccination_date.notnull())
                .group_by(['current_management_code', 'vaccination_name'])
                .agg(vac=_.id_x.nunique())
            )

            result = (
                actual.join(expected, 'current_management_code')
                .mutate(percent=(_.vac / _.cnt * 100).cast(dt.Decimal(10, 2)))
            )

        return result


    def coverage(self):
        recommendations = self.analyze().mutate(age=_.age_month)
        recommendations_df = recommendations.to_pandas()
        
        coverage_data = []
        
        # 定义不同疫苗的计算条件
        vaccine_conditions = [
            {
                'vaccines': ['卡介苗_1'],
                'actual_min_age': 0, 'actual_max_age': 4*12,
                'expected_min_age': 0, 'expected_max_age': 4*12,
            },
            {
                'vaccines': ['乙肝疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 0, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['乙肝疫苗_2'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 1, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['乙肝疫苗_3'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 6, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['脊灰疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 2, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['脊灰疫苗_2'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 3, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['脊灰疫苗_3'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 4, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['脊灰疫苗_4'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 4*12, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['百白破疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 3, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['百白破疫苗_2'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 4, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['百白破疫苗_3'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 5, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['百白破疫苗_4'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 18, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['含麻疹成分疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 8, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['含麻疹成分疫苗_2'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 18, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['乙脑疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 8, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['乙脑疫苗_2'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 2*12, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['甲肝疫苗_1'],
                'actual_min_age': 0, 'actual_max_age': 18*12,
                'expected_min_age': 18, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['流脑疫苗A群_1'],
                'actual_min_age': 0, 'actual_max_age': 2*12,
                'expected_min_age': 6, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['流脑疫苗A群_2'],
                'actual_min_age': 0, 'actual_max_age': 2*12,
                'expected_min_age': 9, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['流脑疫苗AC群_1'],
                'actual_min_age': 2*12, 'actual_max_age': 18*12,
                'expected_min_age': 2*12, 'expected_max_age': 6*12,
            },
            {
                'vaccines': ['流脑疫苗AC群_2'],
                'actual_min_age': 5*12, 'actual_max_age': 18*12,
                'expected_min_age': 5*12, 'expected_max_age': 18*12,
            }
        ]

        # 计算当月的最后一天
        last_day_of_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd.offsets.MonthEnd(0)

        # 计算前12个月的第一天
        first_day_of_month = pd.Timestamp(current_date) - relativedelta(months=12, day=1)
        
        for condition in vaccine_conditions:
            # 计算实种数
            mask_actual = (
                (recommendations_df['vaccination_date'].dt.year == self.current_date.year) &
                (recommendations_df['vaccination_date'].dt.month == self.current_date.month) &
                (recommendations_df['age'] >= condition['actual_min_age']) &
                (recommendations_df['age'] < condition['actual_max_age']) &
                (recommendations_df['vaccination_name'].isin(condition['vaccines'])) &
                (recommendations_df['vaccination_date'].notna()) &
                (recommendations_df['vaccination_org']==recommendations_df['current_management_code'])
            )
            
            actual = (
                recommendations_df[mask_actual]
                .groupby(['vaccination_name', 'vaccination_org'])
                .agg(actual_count=('id_x', 'nunique'))
                .reset_index()
            )
           
            # 计算应种数
            mask_expected = ( 
                (recommendations_df['recommended_date']>= first_day_of_month) &
                (recommendations_df['recommended_date']<= last_day_of_month) &  
                (recommendations_df['age'] >= condition['expected_min_age']) & 
                (recommendations_df['age'] < condition['expected_max_age']) &
                (recommendations_df['vaccination_name'].isin(condition['vaccines'])))
            
            expected = (
                recommendations_df[mask_expected]
                .groupby(['vaccination_name', 'current_management_code'])
                .agg(expected_count=('id_x', 'nunique'))
                .reset_index()
            )
            
            # 合并实种数和应种数，计算接种率
            result = (
                expected
                .merge(actual, 
                       left_on=['vaccination_name', 'current_management_code'],
                       right_on=['vaccination_name', 'vaccination_org'],
                       how='left')
                .fillna(0)
                .assign(
                    prop=lambda x: (x['actual_count'] / (x['expected_count'] + x['actual_count']) * 100).round(2)
                )
            )
            
            coverage_data.append(result)
        
            # 将所有结果合并为一个DataFrame
        combined_coverage = pd.concat(coverage_data, ignore_index=True)
    
        # 重新排列列的顺序，使其更加清晰
        combined_coverage = combined_coverage[[
            'vaccination_name', 'current_management_code', 'expected_count', 
            'actual_count', 'prop'
        ]]
    
        return combined_coverage



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
    .filter([_.id_x=='b7fc099a294047f4b2935988b2664a7a'])
    # .filter([_.current_management_code=='334878619388'])
)
analyzer = VaccinationAnalyzer(person, current_date)

# 获取合并后的结果
# combined_result = analyzer.coverage()

recommendations_df=analyzer.coverage2()


#############################################
# last_day_of_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd.offsets.MonthEnd(0)
# first_day_of_month = pd.Timestamp(current_date) - relativedelta(months=12, day=1)

# condition=vaccine_conditions[0]

# # 计算应种数
# mask_expected = ( 
#     (recommendations_df['recommended_date']>= first_day_of_month) &
#     (recommendations_df['recommended_date']<= last_day_of_month) &  
#     (recommendations_df['age'] >= condition['expected_min_age']) & 
#     (recommendations_df['age'] < condition['expected_max_age']) &
#     (recommendations_df['vaccination_name'].isin(condition['vaccines'])))

# expected = (
#     recommendations_df[mask_expected]
# )


# yingzhong=(
#     pd.read_excel("/mnt/d/标准库接种率/doc/标准库国家计算结果（默认版本）/机构数据334878619388/期间接种率/接种率明细数据/334878619388-2021-01-01 00_00_00-2021-01-31 23_59_59 期间接种率明细.xlsx",sheet_name='卡介苗')
#     .query("index>=7")
# )

# tmp = yingzhong.loc[~yingzhong['个案ID'].isin(expected['id_x'])]

# tmp2=recommendations_df.loc[recommendations_df['id_x'].isin(tmp['个案ID'])]

# tmp2=person.filter(_.id_x.isin(tmp['个案ID'])).to_pandas()

# analyzer = VaccinationAnalyzer(tmp2, current_date)