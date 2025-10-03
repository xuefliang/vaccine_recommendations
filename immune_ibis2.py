import ibis
from ibis.expr import datatypes
from datetime import datetime
from typing import List
from dateutil.relativedelta import relativedelta
from scipy.stats import binomtest

class Immune:
    def __init__(self, df: ibis.expr.types.Table, current_date: ibis.expr.types.TimestampScalar):
        self._validate_dataframe(df)
        self.df = df
        self.current_date = current_date
        self.vaccines = {
            "乙肝疫苗": {
                "doses": 3,
                "schedule": [0, 1, 6],
                "min_interval": self._get_hep_b_intervals,
            },
            "卡介苗": {
                "doses": 1,
                "schedule": [0],
                "min_interval": [0],
            },
            "脊灰疫苗": {
                "doses": 4,
                "schedule": [2, 3, 4, 48],
                "min_interval": [0, 1, 1, 1],
            },
            "百白破疫苗": {
                "doses": 4,
                "schedule": [3, 4, 5, 18],
                "min_interval": [0, 1, 1, 6],
            },
            "含麻疹成分疫苗": {
                "doses": 2,
                "schedule": [8, 18],
                "min_interval": [0, 10]
            },
            "流脑疫苗A群": {
                "doses": 2,
                "schedule": [6, 9],
                "min_interval": [0, 3]
            },
            "流脑疫苗AC群": {
                "doses": 2,
                "schedule": [36, 72],
                "min_interval": [0, 36]
            },
            "乙脑疫苗": {
                "doses": 2,
                "schedule": [8, 24],
                "min_interval": [0, 12]
            },
            "甲肝疫苗": {
                "doses": 1,
                "schedule": [18],
                "min_interval": [0]
            }
        }
        
        self.vaccine_code_map = {
            '0101': '卡介苗',
            '0201': '乙肝疫苗', '0202': '乙肝疫苗', '0203': '乙肝疫苗', '2001': '乙肝疫苗',
            '0301': '脊灰疫苗', '0302': '脊灰疫苗', '0303': '脊灰疫苗', '0304': '脊灰疫苗',
            '0305': '脊灰疫苗', '0306': '脊灰疫苗', '0311': '脊灰疫苗', '0312': '脊灰疫苗',
            '0401': '百白破疫苗', '0402': '百白破疫苗', '0403': '百白破疫苗', '4901': '百白破疫苗',
            '1201': '含麻疹成分疫苗', '1301': '含麻疹成分疫苗', '1401': '含麻疹成分疫苗',
            '5001': 'DTaP-IPV-Hib五联疫苗',
            '0601': '白破疫苗',
            '1601': '流脑疫苗A群',
            '1701': '流脑疫苗AC群', 
            '1702': '流脑替代疫苗', '1703': '流脑替代疫苗', '1704': '流脑替代疫苗', '5301': '流脑替代疫苗',
            '1801': '乙脑疫苗', 
            '1802': '乙脑灭活疫苗', '1803': '乙脑灭活疫苗', '1804': '乙脑灭活疫苗',
            '1901': '甲肝疫苗', 
            '1902': '甲肝灭活疫苗', '1903': '甲肝灭活疫苗'
        }

    def _validate_dataframe(self, df: ibis.expr.types.Table):
        required_columns = {
            'id_x': ibis.expr.datatypes.String,
            'birth_date': ibis.expr.datatypes.Timestamp,
            'current_management_code': ibis.expr.datatypes.String,
            'vaccination_date': ibis.expr.datatypes.Timestamp,
            'vaccination_code': ibis.expr.datatypes.String,
            'vaccination_org': ibis.expr.datatypes.String,
            'entry_org': ibis.expr.datatypes.String
        }

        missing_columns = [col for col in required_columns if col not in df.columns]
        type_errors = []

        for col, expected_type in required_columns.items():
            if col in df.columns:
                actual_type = df[col].type()
                if not isinstance(actual_type, expected_type):
                    type_errors.append(
                        f"列 '{col}' 的类型应为 {expected_type().name}, 但实际为 {actual_type.name}"
                    )

        error_messages = []
        if missing_columns:
            error_messages.append(f"缺少以下必需的列：{', '.join(missing_columns)}")
        if type_errors:
            error_messages.append("类型错误：" + "; ".join(type_errors))

        if error_messages:
            raise ValueError("; ".join(error_messages))

    def _add_months(self,date:ibis.expr.types.TimestampScalar, months:int):
        return date + relativedelta(months=months)

    def _get_hep_b_intervals(self, birth_date: ibis.expr.types.TimestampScalar) -> List[int]:
        age_months=relativedelta(self.current_date, birth_date).months+relativedelta(self.current_date, birth_date).years*12
        return ibis.case()\
            .when(age_months < 12, [0, 1, 5])\
            .else_([0, 1, 3])

    def get_recommended_dates(self, vaccine_name: str, birth_date: ibis.expr.types.TimestampValue, vaccinations: ibis.expr.types.ColumnExpr) -> ibis.expr.types.ColumnExpr:
        if vaccine_name not in self.vaccines:
            return ibis.NA

        vaccine_info = self.vaccines[vaccine_name]
        recommended_dates = []
        
        min_intervals = (vaccine_info["min_interval"](birth_date) 
                         if callable(vaccine_info["min_interval"]) 
                         else vaccine_info["min_interval"])

        schedule = vaccine_info["schedule"]

        for i in range(0, vaccine_info["doses"]):
            if i == 0:
                recommended_date = ibis.case()\
                    .when((vaccinations[0].isnull()) | (vaccinations[0] - birth_date < min_intervals[0]) | (vaccinations[0] > self.current_date),
                         birth_date + relativedelta(months=schedule[i]))\
                    .else_(ibis.NA)
            else:
                recommended_date = ibis.case()\
                    .when((vaccinations[i].notnull()) & (vaccinations[i] < self.current_date), ibis.NA)\
                    .when((i > 0) & (vaccinations[i-1].notnull()),
                         ibis.greatest(vaccinations[i-1] + relativedelta(months=min_intervals[i]), birth_date + relativedelta(months=schedule[i])))\
                    .else_(ibis.NA)
            
            recommended_dates.append(recommended_date)

        return ibis.array(recommended_dates)

    def _process_vaccination_code(self):
        # 处理 vaccination_code 为 '5001' 的记录
        code_5001 = self.df.filter(self.df.vaccination_code == '5001')
        
        # 创建新的行，修改 vaccination_code
        new_rows_0301 = code_5001.mutate(vaccination_code=ibis.literal('0301'))
        new_rows_0401 = code_5001.mutate(vaccination_code=ibis.literal('0401'))
        
        # 将新行添加到原始 DataFrame
        self.df = self.df.union(new_rows_0301).union(new_rows_0401)

    ## 生成各类疫苗一下针的推荐时间
    def generate_recommendations(self) -> ibis.expr.types.Table:
        # 新增调用处理方法
        self._process_vaccination_code()
        # 生成初始建议
        recommendations = self._generate_initial_recommendations()
        return recommendations

    def _generate_initial_recommendations(self) -> ibis.expr.types.Table:
        # 使用 Ibis 的方式来映射 vaccination_code 到 vaccine_name
        vaccine_name_mapping = ibis.case()
        for code, name in self.vaccine_code_map.items():
            vaccine_name_mapping = vaccine_name_mapping.when(self.df.vaccination_code == code, ibis.literal(name))
        vaccine_name_mapping = vaccine_name_mapping.else_(self.df.vaccination_code).end()
        
        self.df = self.df.mutate(vaccine_name=vaccine_name_mapping.cast('string'))

        # 预处理数据
        grouped_data = self.df.group_by('id_x')
        
        recommendations = []
        
        # 使用 Ibis 的聚合函数来处理分组数据
        aggregated_data = grouped_data.aggregate(
            birth_date=self.df.birth_date.first(),
            latest_management_code=self.df.current_management_code.last()
        )
        
        # 在执行后计算年龄
        aggregated_data = aggregated_data.execute()
        aggregated_data['current_age_months'] = (self.current_date.year - aggregated_data.birth_date.year) * 12 + \
                                                (self.current_date.month - aggregated_data.birth_date.month)
        
        for row in aggregated_data.itertuples():
            id_x = row.id_x
            birth_date = row.birth_date
            current_age_months = row.current_age_months
            latest_management_code = row.latest_management_code
            
            individual_data = self.df[self.df.id_x == id_x]

            for vaccine_name, vaccine_info in self.vaccines.items():
                vaccine_data = individual_data[individual_data.vaccine_name == vaccine_name]
                vaccine_entry_org = vaccine_data.entry_org.first().execute()

                vaccinations = vaccine_data.vaccination_date.sort().execute().tolist()
                
                recommended_dates = self.get_recommended_dates(vaccine_name, birth_date, vaccinations)
                
                for dose in range(1, vaccine_info['doses'] + 1):
                    recommended_date = recommended_dates[dose - 1]
                    if dose <= len(vaccinations):
                        date = vaccinations[dose - 1]
                        vacc_month = (date - birth_date).days() // 30
                        current_vaccination = vaccine_data[vaccine_data.vaccination_date == date]
                        vaccination_org = current_vaccination.vaccination_org.first().execute()
                    else:
                        date = None
                        vacc_month = None
                        vaccination_org = None

                    recommendations.append({
                        'id_x': id_x,
                        'vaccine_name': vaccine_name,
                        'dose': dose,
                        'vaccination_date': date,
                        'recommended_date': recommended_date,
                        'current_management_code': latest_management_code,
                        'vaccination_org': vaccination_org,
                        'vacc_month': vacc_month,
                        'age': current_age_months,
                        'entry_org': vaccine_entry_org,
                        'birth_date': birth_date
                    })

        recommendations = ibis.duckdb.connect({'recommendations': pd.DataFrame(recommendations)}).table('recommendations')
        
        # 更新满足条件的vaccination_org
        mask = (recommendations.vaccination_org.isin(['777777777777', '888888888888', '999999999999'])) & \
            (recommendations.vaccine_name.isin(['乙肝疫苗', '卡介苗'])) & \
            (recommendations.dose == 1)
        
        recommendations = recommendations.mutate(
            vaccination_org=ibis.case().when(mask, recommendations.entry_org).else_(recommendations.vaccination_org).end()
        ).drop('entry_org')
        
        return recommendations
    
    def get_first_dose_date(self, vaccine_name: str, birth_date: ibis.expr.types.TimestampScalar) -> ibis.expr.types.TimestampScalar:
        vaccine_info = self.vaccines[vaccine_name]
        min_intervals = (vaccine_info["min_interval"](birth_date) 
                         if callable(vaccine_info["min_interval"]) 
                         else vaccine_info["min_interval"])
        return birth_date + relativedelta(months=min_intervals[0])
    
    def other_vaccines(self) -> ibis.expr.types.Table:
        if 'vaccine_name' not in self.df.columns:
            self.df = self.df.mutate(vaccine_name=self.df.vaccination_code.map(lambda x: self.vaccine_code_map.get(x, x)))
        
        other_vaccines = self.df[~self.df.vaccine_name.isin(self.vaccines.keys())]
        
        if other_vaccines.count().execute() == 0:
            return ibis.NA  # 如果没有其他疫苗，返回空的DataFrame
        
        results = []
        for (id_x, vaccine_name), group in other_vaccines.group_by(['id_x', 'vaccine_name']).iterate():
            birth_date = group.birth_date.first()
            current_age_months = (self.current_date - birth_date).days() // 30
            latest_management_code = group.current_management_code.last()
            vaccine_entry_org = group.entry_org.first()

            # 按vaccination_date排序
            sorted_group = group.sort_by('vaccination_date')

            for i, row in enumerate(sorted_group.iterate(), start=1):
                vaccination_date = row.vaccination_date
                vacc_month = (vaccination_date - birth_date).days() // 30
                
                results.append({
                    'id_x': id_x,
                    'vaccine_name': vaccine_name,
                    'dose': i,  # 现在dose是按vaccination_date排序的
                    'vaccination_date': vaccination_date,
                    'recommended_date': None,
                    'current_management_code': latest_management_code,
                    'vaccination_org': row.vaccination_org,
                    'vacc_month': vacc_month,
                    'age': current_age_months,
                    'entry_org': vaccine_entry_org,
                    'birth_date': birth_date
                })

        return ibis.duckdb.connect({'results': pd.DataFrame(results)}).table('results')
    
    def process_vaccines(self):
        def process_group(group):
            # 处理流脑疫苗第一种情况
            condition1 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month < 6)
            if condition1.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition1, '流脑疫苗A群').else_(group.vaccine_name),
                    dose=ibis.case().when(condition1, 1).when((group.vaccine_name == '流脑替代疫苗') & (group.dose == 3), 2).else_(group.dose)
                )
            
            # 处理第二种情况
            condition2 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month >= 6) & (group.vacc_month < 24)
            if condition2.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition2, '流脑疫苗A群').else_(group.vaccine_name)
                )
            
            # 处理第三种情况
            condition3 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month >= 24)
            if condition3.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition3, '流脑疫苗AC群').else_(group.vaccine_name)
                )
            
            # 处理第四种情
            condition4 = (group.vaccine_name == '流脑替代疫苗') & (group.vacc_month >= 5*12)
            if condition4.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition4, '流脑疫苗AC群').else_(group.vaccine_name)
                )
            
            # 新增：处理甲肝灭活疫苗的情况
            condition_hepA = (group.vaccine_name == '甲肝灭活疫苗') & (group.dose == 1)
            if condition_hepA.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition_hepA, '甲肝疫苗').else_(group.vaccine_name)
                )
            
            # 新增：处理乙脑灭活疫苗的情况
            # 情况1：dose == 2 时
            condition_je1 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 2)
            if condition_je1.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition_je1, '乙脑疫苗').else_(group.vaccine_name),
                    dose=ibis.case().when(condition_je1, 1).else_(group.dose)
                )

            # 情况2：dose == 3 且 vacc_month < 6*12 时
            condition_je2 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 3) & (group.vacc_month < 6*12)
            if condition_je2.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition_je2, '乙脑疫苗').else_(group.vaccine_name),
                    dose=ibis.case().when(condition_je2, 2).else_(group.dose)
                )

            # 情况3：dose == 4 且 vacc_month >= 6*12 时
            condition_je3 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 4) & (group.vacc_month >= 6*12)
            if condition_je3.any().execute():
                group = group.mutate(
                    vaccine_name=ibis.case().when(condition_je3, '乙脑疫苗').else_(group.vaccine_name),
                    dose=ibis.case().when(condition_je3, 2).else_(group.dose)
                )

            return group

        # 按 id_x 分组并应用处理函数
        processed_vaccines = self.other_vaccines().group_by('id_x').mutate(process_group)
        return processed_vaccines
    
    def merge_recommendations(self) -> ibis.expr.types.Table:
        # 获取推荐和其他疫苗的数据
        recommendations = self.generate_recommendations()
        other_vaccines = self.process_vaccines()

        # 合并数据
        merged = recommendations.join(
            other_vaccines[['id_x', 'vaccine_name', 'dose', 'vaccination_date', 'vaccination_org', 'vacc_month']],
            ['id_x', 'vaccine_name', 'dose'],
            how='left',
            suffixes=('', '_other')
        )

        # 更新相应的列
        columns_to_update = ['vaccination_date', 'vaccination_org', 'vacc_month']
        for col in columns_to_update:
            merged = merged.mutate(**{col: merged[f'{col}_other'].fillna(merged[col])}).drop(f'{col}_other')

        merged = self._update_ac_recommendations(merged)
        merged = self._update_recommendations(merged)
        return merged
    
    def _update_ac_recommendations(self, recommendations_df: ibis.expr.types.Table) -> ibis.expr.types.Table:
        grouped = recommendations_df.group_by('id_x')
        updated_recommendations = []
        
        for _, group in grouped.iterate():
            birth_date = group.birth_date.first()
            current_age_months = group.age.first()
            
            # 获取流脑疫苗A群和AC群的数据
            a_group = group[group.vaccine_name == '流脑疫苗A群']
            ac_group = group[group.vaccine_name == '流脑疫苗AC群']
            
            # 检查流脑疫苗A群的第1剂和第2剂是否都已接种
            a_group_doses_completed = (a_group.dose.isin([1, 2]) & a_group.vaccination_date.notnull()).all().execute()
            a_group_dose1_completed = (a_group.dose == 1).any().execute() and (a_group[a_group.dose == 1].vaccination_date.notnull().any().execute())
            
            # 新增：检查流脑疫苗A群接种2针的时的年龄是否均小于2*12
            a_group_doses_under_24_months = (a_group.vacc_month < 24).all().execute()
            
            # 处理流脑疫苗AC群的接种日期
            for idx, row in ac_group.iterate():
                if row.dose == 1:
                    if current_age_months >= 3*12 and current_age_months < 6*12 and a_group_doses_completed:
                        ac_group = ac_group.mutate(
                            recommended_date=ibis.case().when(ac_group.dose == 1, self._add_months(birth_date, 36)).else_(ac_group.recommended_date)
                        )
                    elif current_age_months >= 2*12 and current_age_months < 3*12 and a_group_doses_completed:
                        # 新增：当A群接种2针且年龄在24-36个月之间时，不计算recommended_date
                        ac_group = ac_group.mutate(
                            recommended_date=ibis.case().when(ac_group.dose == 1, None).else_(ac_group.recommended_date)
                        )
                    elif a_group_doses_under_24_months:
                        ac_group = ac_group.mutate(
                            recommended_date=ibis.case().when(ac_group.dose == 1, None).else_(ac_group.recommended_date)
                        )
                    elif current_age_months >= 2*12 and a_group_dose1_completed:
                        a_group_dose2 = a_group[a_group.dose == 2]
                        if a_group_dose2.count().execute() > 0 and a_group_dose2.vaccination_date.notnull().any().execute():
                            ac_group = ac_group.mutate(
                                recommended_date=ibis.case().when(ac_group.dose == 1, self._add_months(a_group_dose2.vaccination_date.first(), 3)).else_(ac_group.recommended_date)
                            )
                        else:
                            ac_group = ac_group.mutate(
                                recommended_date=ibis.case().when(ac_group.dose == 1, self._add_months(birth_date, 24)).else_(ac_group.recommended_date)
                            )
                    elif current_age_months >= 2*12 and not a_group_doses_completed:
                        ac_group = ac_group.mutate(
                            recommended_date=ibis.case().when(ac_group.dose == 1, self._add_months(birth_date, 24)).else_(ac_group.recommended_date)
                        )

                elif row.dose == 2:
                    ac_dose_1 = ac_group[ac_group.dose == 1]
                    if ac_dose_1.count().execute() > 0 and ac_dose_1.vaccination_date.notnull().any().execute():
                        vaccination_date = ac_dose_1.vaccination_date.first()
                        if vaccination_date.notnull().execute():
                            ac_group = ac_group.mutate(
                                recommended_date=ibis.case().when(ac_group.dose == 2, self._add_months(vaccination_date, 36)).else_(ac_group.recommended_date)
                            )
            
            # 更新组内的AC群数据
            group = group.mutate(
                vaccine_name=ibis.case().when(group.vaccine_name == '流脑疫苗AC群', ac_group.vaccine_name).else_(group.vaccine_name),
                dose=ibis.case().when(group.vaccine_name == '流脑疫苗AC群', ac_group.dose).else_(group.dose),
                recommended_date=ibis.case().when(group.vaccine_name == '流脑疫苗AC群', ac_group.recommended_date).else_(group.recommended_date)
            )
            
            updated_recommendations.append(group)
        
        return ibis.duckdb.connect({'updated_recommendations': pd.concat([x.to_pandas() for x in updated_recommendations], ignore_index=True)}).table('updated_recommendations')
    
    def _update_recommendations(self, recommendations_df: ibis.expr.types.Table) -> ibis.expr.types.Table:
        # 遍历每一行数据
        for idx, row in recommendations_df.iterate():
            # 检查vaccination_date是否current_date之前
            if row.vaccination_date.notnull().execute() and row.vaccination_date < self.current_date:
                # 将recommended_date置空
                recommendations_df = recommendations_df.mutate(
                    recommended_date=ibis.case().when(recommendations_df.index == idx, None).else_(recommendations_df.recommended_date)
                )
        return recommendations_df

    @staticmethod
    def calculate_ci(k: ibis.expr.types.IntegerScalar, n: ibis.expr.types.IntegerScalar, confidence_level=0.95):
        bino = binomtest(k=k, n=n, p=0.05, alternative='two-sided')
        ci = bino.proportion_ci(confidence_level=confidence_level)
        return ibis.literal({'lower_ci': ci.low * 100, 'upper_ci': ci.high * 100})

    def cohort_rate(self, by_age=False, calculate_ci=False) -> ibis.expr.types.Table:
        recommendations = self.merge_recommendations()
        recommendations = recommendations.mutate(age=recommendations.age // 12)

        if by_age:
            expected = (
                recommendations
                .filter(recommendations.age >= 1)
                .group_by(['current_management_code', 'age'])
                .aggregate(cnt=recommendations.id_x.nunique())
            )

            actual = (
                recommendations
                .filter((recommendations.age >= 1) & recommendations.vaccination_date.notnull())
                .group_by(['current_management_code', 'age', 'vaccine_name', 'dose'])
                .aggregate(vac=recommendations.id_x.nunique())
            )

            result = actual.join(expected, ['current_management_code', 'age'], how='inner').mutate(
                percent=lambda x: (x.vac / x.cnt * 100).round(2)
            )

        else:
            expected = (
                recommendations
                .filter(recommendations.age >= 1)
                .group_by(['current_management_code'])
                .aggregate(cnt=recommendations.id_x.nunique())
            )

            actual = (
                recommendations
                .filter((recommendations.age >= 1) & recommendations.vaccination_date.notnull())
                .group_by(['current_management_code', 'vaccine_name', 'dose'])
                .aggregate(vac=recommendations.id_x.nunique())
            )

            result = actual.join(expected, ['current_management_code'], how='inner').mutate(
                percent=lambda x: (x.vac / x.cnt * 100).round(2)
            )

        if calculate_ci:
            result = result.mutate(
                ci_result=result.apply(
                    lambda row: self.calculate_ci(row.vac.cast('int'), row.cnt.cast('int'), confidence_level=0.95),
                    axis=1
                )
            )
            result = result.mutate(
                lower_ci=result.ci_result.get('lower_ci'),
                upper_ci=result.ci_result.get('upper_ci')
            ).drop('ci_result')
        return result
    
    def generate_coverage(self, calculate_ci=False) -> ibis.expr.types.Table:
        recommendations_df = self.merge_recommendations()
        
        coverage_data = []
        
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
        
        for condition in vaccine_conditions:
            # 计算实种数
            mask_actual = (
                (recommendations_df.vaccination_date.year() == self.current_date.year()) &
                (recommendations_df.vaccination_date.month() == self.current_date.month()) &
                (recommendations_df.age >= condition['actual_min_age']) &
                (recommendations_df.age < condition['actual_max_age']) &
                (recommendations_df.vaccine_name.isin(condition['vaccines'])) &
                (recommendations_df.dose.isin(condition['doses'])) &
                (recommendations_df.vaccination_date.notnull()) &
                (recommendations_df.vaccination_org == recommendations_df.current_management_code)
            )
            
            actual = (
                recommendations_df[mask_actual]
                .group_by(['vaccine_name', 'vaccination_org', 'dose'])
                .agg(actual_count=('id_x', 'nunique'))
                .reset_index()
            )
            
            # 计算应种数
            mask_expected = (
                (recommendations_df.recommended_date.year() * 12 + recommendations_df.recommended_date.month()) >= 
                ((self.current_date - ibis.interval(months=12)).year() * 12 + (self.current_date - ibis.interval(months=12)).month())
            ) & (
                (recommendations_df.recommended_date.year() * 12 + recommendations_df.recommended_date.month()) <= 
                (self.current_date.year() * 12 + self.current_date.month())
            ) & (
                (recommendations_df.age >= condition['expected_min_age']) & 
                (recommendations_df.age < condition['expected_max_age']) &
                (recommendations_df.vaccine_name.isin(condition['vaccines'])) &
                (recommendations_df.dose.isin(condition['doses']))
            )
            
            expected = (
                recommendations_df[mask_expected]
                .group_by(['vaccine_name', 'current_management_code', 'dose'])
                .agg(expected_count=('id_x', 'nunique'))
                .reset_index()
            )
            
            # 合并实种数和应种数，计算接种率
            result = (
                expected
                .join(actual, 
                       ['vaccine_name', 'vaccination_org', 'dose'],
                       how='left')
                .fillna(0)
                .mutate(
                    prop=lambda x: (x.actual_count / (x.expected_count + x.actual_count) * 100).round(2)
                )
            )
            
            coverage_data.append(result)
        
        # 合并所有结果
        cover = ibis.duckdb.connect({'cover': pd.concat([x.to_pandas() for x in coverage_data], ignore_index=True)}).table('cover')
        if calculate_ci:
            cover = cover.mutate(
                ci_result=cover.apply(
                    lambda row: self.calculate_ci(row.actual_count.cast('int'), (row.actual_count + row.expected_count).cast('int'), confidence_level=0.95),
                    axis=1
                )
            )
            cover = cover.mutate(
                lower_ci=cover.ci_result.get('lower_ci'),
                upper_ci=cover.ci_result.get('upper_ci')
            ).drop('ci_result')
        return cover

import ibis
from ibis import _
import ibis.selectors as s
from ibis import _
ibis.options.interactive = True

# 创建一个内存数据库连接
con = ibis.duckdb.connect()
person=con.read_csv(r'/mnt/d/标准库接种率/data/person2.csv')

# # 读取CSV文件
# person = con.read_csv(r'/mnt/d/标准库接种率/data/person_standard.csv')
# vacc = con.read_csv(r'/mnt/d/标准库接种率/data/person_standard_vaccination.csv')

# # 合并数据
# person2 = person.left_join(vacc, [person.ID == vacc.ID])

# # 重命名列
# person2 = person2.relabel(dict(zip(person2.columns, [x.lower() for x in person2.columns])))
# person = person.rename(dict(zip(_.columns, [x.lower() for x in _.columns])))

# 转换数据类型
person = person.mutate(
    birth_date=_.birth_date.cast('timestamp'),
    vaccination_date=_.vaccination_date.cast('timestamp'),
    current_management_code=_.current_management_code.cast('string'),
    vaccination_org=_.vaccination_org.cast('string'),
    entry_org=_.entry_org.cast('string')
).filter([_.current_management_code=='334878619388'])

# 创建Immune实例
immune = Immune(person, ibis.literal('2021-01-15').cast('timestamp'))
cover=immune.merge_recommendations()