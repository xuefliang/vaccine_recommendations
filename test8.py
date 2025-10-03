import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from functools import reduce

class VaccinationAnalyzer:
    def __init__(self, person, current_date):
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
        self.person['vaccine_name'] = self.person['vaccination_code'].map(self.vaccine_mapping)
        self.person['current_date'] = pd.to_datetime(self.current_date)
        self.person['vaccination_month'] = (self.person['vaccination_date'] - self.person['birth_date']).dt.days / 30.44
        self.person['age_month'] = (self.person['current_date'] - self.person['birth_date']).dt.days / 30.44
        
        self.person = self.person.sort_values(['id_x', 'vaccine_name', 'vaccination_date'])
        self.person['dose'] = (self.person.groupby(['id_x', 'vaccine_name']).cumcount() + 1).astype(str)
        
        mask = (self.person['vaccination_org'].isin(['777777777777', '888888888888', '999999999999'])) & \
               (self.person['vaccine_name'].isin(['乙肝疫苗', '卡介苗'])) & \
               (self.person['dose'] == 1)
        self.person.loc[mask, 'vaccination_org'] = self.person.loc[mask, 'entry_org']
        
        conditions = [
            (self.person['vaccine_name'] == '卡介苗') & (self.person['dose'] == 1),
            (self.person['vaccine_name'] == '乙肝疫苗') & self.person['dose'].isin([1, 2, 3]),
            (self.person['vaccine_name'] == '脊灰疫苗') & self.person['dose'].isin([1, 2, 3, 4]),
            (self.person['vaccine_name'] == '百白破疫苗') & self.person['dose'].isin([1, 2, 3, 4]),
            (self.person['vaccine_name'] == '含麻疹成分疫苗') & self.person['dose'].isin([1, 2]),
            (self.person['vaccine_name'] == 'DTaPIPVHib五联疫') & self.person['dose'].isin([1, 2, 3, 4]),
            (self.person['vaccine_name'] == '流脑疫苗A群') & self.person['dose'].isin([1, 2]),
            (self.person['vaccine_name'] == '流脑疫苗AC群') & self.person['dose'].isin([1, 2]),
            (self.person['vaccine_name'] == '流脑替代疫苗') & self.person['dose'].isin([1, 2, 3, 4]),
            (self.person['vaccine_name'] == '乙脑疫苗') & self.person['dose'].isin([1, 2]),
            (self.person['vaccine_name'] == '乙脑灭活疫苗') & self.person['dose'].isin([1, 2, 3, 4]),
            (self.person['vaccine_name'] == '甲肝疫苗') & self.person['dose'].isin([1]),
            (self.person['vaccine_name'] == '甲肝灭活疫苗') & self.person['dose'].isin([1, 2])
        ]
        
        combined_condition = reduce(lambda x, y: x | y, conditions)
        self.person = self.person[combined_condition]
        
        self.vaccination_org = self.person[['id_x', 'vaccine_name', 'dose', 'vaccination_org','age_month']]
        self.vaccination_org['vaccine_name'] = self.vaccination_org['vaccine_name'] + '_' + self.vaccination_org['dose'].astype(str)
        self.vaccination_org = self.vaccination_org.drop(columns=['dose'])

    def pivot_data(self):
        preserved_columns = self.person[['id_x','birth_date','current_management_code', 'vaccination_code', 'vaccination_month','age_month']].drop_duplicates()
        
        self.person = self.person.pivot(index='id_x', columns=['dose', 'vaccine_name'], values='vaccination_date')
        self.person.columns = [f"{col[1]}_{col[0]}" for col in self.person.columns]
        self.person = self.person.reset_index()
        
        self.person = self.person.merge(preserved_columns, on='id_x', how='left')
        
        for col in self.expected_columns:
            if col not in self.person.columns:
                self.person[col] = pd.NaT
        

        columns_to_keep = ['id_x'] + self.expected_columns + ['birth_date','current_management_code','vaccination_code', 'vaccination_month','age_month']
        self.person = self.person[columns_to_keep]

    def substitute_vaccines(self):
        self.person['脊灰疫苗_1'] = self.person['脊灰疫苗_1'].fillna(self.person['DTaPIPVHib五联疫苗_1'])
        self.person['脊灰疫苗_2'] = self.person['脊灰疫苗_2'].fillna(self.person['DTaPIPVHib五联疫苗_2'])
        self.person['脊灰疫苗_3'] = self.person['脊灰疫苗_3'].fillna(self.person['DTaPIPVHib五联疫苗_3'])
        self.person['脊灰疫苗_4'] = self.person['脊灰疫苗_4'].fillna(self.person['DTaPIPVHib五联疫苗_4'])
        self.person['流脑疫苗A群_1'] = self.person['流脑疫苗A群_1'].fillna(self.person['流脑替代疫苗_1'])
        
        mask = (self.person['vaccination_code'] == '5001') & (self.person['vaccination_month'] < 6)
        self.person.loc[mask, '流脑疫苗A群_2'] = self.person.loc[mask, '流脑疫苗A群_2'].fillna(self.person.loc[mask, '流脑替代疫苗_3'])
        
        mask = (self.person['vaccination_code'] == '5001') & (self.person['vaccination_month'] >= 6)
        self.person.loc[mask, '流脑疫苗A群_2'] = self.person.loc[mask, '流脑疫苗A群_2'].fillna(self.person.loc[mask, '流脑替代疫苗_2'])
        
        mask = (self.person['vaccination_code'] == '5001') & (self.person['vaccination_month'] >= 24)
        self.person.loc[mask, '流脑疫苗AC群_1'] = self.person.loc[mask, '流脑疫苗AC群_1'].fillna(self.person.loc[mask, '流脑替代疫苗_1'])
        
        mask = (self.person['vaccination_code'] == '5001') & (self.person['vaccination_month'] >= 60)
        self.person.loc[mask, '流脑疫苗AC群_2'] = self.person.loc[mask, '流脑疫苗AC群_2'].fillna(self.person.loc[mask, '流脑替代疫苗_2'])
        
        self.person['甲肝疫苗_1'] = self.person['甲肝疫苗_1'].fillna(self.person['甲肝灭活疫苗_1'])
        
        mask = self.person['vaccination_code'].isin(['1802','1803','1804'])
        self.person.loc[mask, '乙脑疫苗_1'] = self.person.loc[mask, '乙脑疫苗_1'].fillna(self.person.loc[mask, '乙脑灭活疫苗_2'])
        
        mask = (self.person['vaccination_code'].isin(['1802','1803','1804'])) & (self.person['vaccination_month'] <= 72)
        self.person.loc[mask, '乙脑疫苗_2'] = self.person.loc[mask, '乙脑疫苗_2'].fillna(self.person.loc[mask, '乙脑灭活疫苗_3'])
        
        mask = (self.person['vaccination_code'].isin(['1802','1803','1804'])) & (self.person['vaccination_month'] > 72)
        self.person.loc[mask, '乙脑疫苗_2'] = self.person.loc[mask, '乙脑疫苗_2'].fillna(self.person.loc[mask, '乙脑灭活疫苗_4'])

    def calculate_recommended_dates(self):
        self.person['recommended_卡介苗_1'] = self.person['birth_date']
        self.person['recommended_乙肝疫苗_1'] = self.person['birth_date']
        
        mask = self.person['乙肝疫苗_1'].notna()
        self.person.loc[mask, 'recommended_乙肝疫苗_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=1), row['乙肝疫苗_1'] + pd.DateOffset(months=1)), axis=1
        )
        
        mask = self.person['乙肝疫苗_2'].notna()
        self.person.loc[mask, 'recommended_乙肝疫苗_3'] = self.person.loc[mask].apply(
            lambda row: max(
                row['birth_date'] + pd.DateOffset(months=6),
                min(row['乙肝疫苗_1'] + pd.DateOffset(months=6 if row['age_month'] < 12 else 4),
                    row['乙肝疫苗_2'] + pd.DateOffset(months=1 if row['age_month'] < 12 else 2))
            ), axis=1
        )
        
        self.person['recommended_脊灰疫苗_1'] = self.person['birth_date'] + pd.DateOffset(months=2)
        
        mask = self.person['脊灰疫苗_1'].notna()
        self.person.loc[mask, 'recommended_脊灰疫苗_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=3), row['脊灰疫苗_1'] + pd.DateOffset(months=1)), axis=1
        )
        
        mask = self.person['脊灰疫苗_2'].notna()
        self.person.loc[mask, 'recommended_脊灰疫苗_3'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=4), row['脊灰疫苗_2'] + pd.DateOffset(months=1)), axis=1
        )
        
        mask = self.person['脊灰疫苗_3'].notna()
        self.person.loc[mask, 'recommended_脊灰疫苗_4'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=4*12), row['脊灰疫苗_3'] + pd.DateOffset(months=1)), axis=1
        )
        
        self.person['recommended_百白破疫苗_1'] = self.person['birth_date'] + pd.DateOffset(months=3)
        
        mask = self.person['百白破疫苗_1'].notna()
        self.person.loc[mask, 'recommended_百白破疫苗_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=4), row['百白破疫苗_1'] + pd.DateOffset(months=1)), axis=1
        )
        
        mask = self.person['百白破疫苗_2'].notna()
        self.person.loc[mask, 'recommended_百白破疫苗_3'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=5), row['百白破疫苗_2'] + pd.DateOffset(months=1)), axis=1
        )
        
        mask = self.person['百白破疫苗_3'].notna()
        self.person.loc[mask, 'recommended_百白破疫苗_4'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=18), row['百白破疫苗_3'] + pd.DateOffset(months=6)), axis=1
        )
        
        self.person['recommended_含麻疹成分疫苗_1'] = self.person['birth_date'] + pd.DateOffset(months=8)
        
        mask = self.person['含麻疹成分疫苗_1'].notna()
        self.person.loc[mask, 'recommended_含麻疹成分疫苗_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=18), row['含麻疹成分疫苗_1'] + pd.DateOffset(months=1)), axis=1
        )
        
        self.person['recommended_乙脑疫苗_1'] = self.person['birth_date'] + pd.DateOffset(months=8)
        
        mask = self.person['乙脑疫苗_1'].notna()
        self.person.loc[mask, 'recommended_乙脑疫苗_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=2*12), row['乙脑疫苗_1'] + pd.DateOffset(months=12)), axis=1
        )
        
        self.person['recommended_甲肝疫苗_1'] = self.person['birth_date'] + pd.DateOffset(months=18)
        self.person['recommended_流脑疫苗A群_1'] = self.person['birth_date'] + pd.DateOffset(months=6)
        
        mask = self.person['流脑疫苗A群_1'].notna()
        self.person.loc[mask, 'recommended_流脑疫苗A群_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=9), row['流脑疫苗A群_1'] + pd.DateOffset(months=3)), axis=1
        )
        
        mask = (self.person['age_month'] >= 3*12) & (self.person['流脑疫苗A群_1'].notna()) & (self.person['流脑疫苗A群_2'].notna())
        self.person.loc[mask, 'recommended_流脑疫苗AC群_1'] = self.person.loc[mask, 'birth_date'] + pd.DateOffset(months=3*12)
        
        mask = (self.person['age_month'] >= 2*12) & (self.person['流脑疫苗A群_1'].notna()) & (self.person['流脑疫苗A群_2'].isnull())
        self.person.loc[mask, 'recommended_流脑疫苗AC群_1'] = self.person.loc[mask, '流脑疫苗A群_1'] + pd.DateOffset(months=3)
        
        mask = (self.person['age_month'] >= 2*12) & (self.person['流脑疫苗A群_1'].isnull()) & (self.person['流脑疫苗A群_2'].isnull())
        self.person.loc[mask, 'recommended_流脑疫苗AC群_1'] = self.person.loc[mask, 'birth_date'] + pd.DateOffset(months=2*12)
        
        mask = self.person['流脑疫苗AC群_1'].notna()
        self.person.loc[mask, 'recommended_流脑疫苗AC群_2'] = self.person.loc[mask].apply(
            lambda row: max(row['birth_date'] + pd.DateOffset(months=5*12), row['流脑疫苗AC群_1'] + pd.DateOffset(months=3*12)), axis=1
        )

    def prepare_result(self):
        vaccination_org = self.vaccination_org
        
        vaccine_table = pd.DataFrame({'vaccination_name': self.vaccine_names})
        
        self.info = self.person[['id_x','birth_date', 'current_management_code', 'age_month']]
        self.info = self.info.merge(vaccine_table, how='cross')
        
        self.vaccination = self.person[['id_x'] + self.vaccine_names]
        self.vaccination = self.vaccination.melt(id_vars=['id_x'], var_name='vaccination_name', value_name='vaccination_date')
        self.vaccination = self.vaccination.dropna(subset=['vaccination_date'])
        
        recommended_columns = ['recommended_' + name for name in self.vaccine_names]
        self.recommended = self.person[['id_x'] + recommended_columns]
        self.recommended = self.recommended.melt(id_vars=['id_x'], var_name='recommended_name', value_name='recommended_date')
        self.recommended = self.recommended.dropna(subset=['recommended_date'])
        self.recommended['recommended_name'] = self.recommended['recommended_name'].str.replace('recommended_', '')
        
        self.result = (
            self.info.merge(
                self.vaccination,
                on=['id_x', 'vaccination_name'],
                how='left'
            ).merge(
                self.recommended,
                left_on=['id_x', 'vaccination_name'],
                right_on=['id_x', 'recommended_name'],
                how='left'
            ).merge(
                vaccination_org,
                left_on=['id_x', 'vaccination_name'],
                right_on=['id_x', 'vaccine_name'],
                how='left'
            )
        )
        self.result = self.result.drop(columns=['recommended_name', 'vaccine_name'])

    def update_recommended_dates(self):
        mask = (self.result['vaccination_date'] <= self.result['recommended_date']) | \
               (self.result['vaccination_date'] < pd.to_datetime(self.current_date))
        self.result.loc[mask, 'recommended_date'] = pd.NaT

    def analyze(self):
        self.preprocess_data()
        self.pivot_data()
        self.substitute_vaccines()
        self.calculate_recommended_dates()
        self.prepare_result()
        self.update_recommended_dates()
        return self.result

    def cohort_rate(self, by_age=False):
        recommendations = self.analyze()
        recommendations['age'] = recommendations['age_month'] // 12

        if by_age:
            expected = (
                recommendations[recommendations['age'] >= 1]
                .groupby(['current_management_code', 'age'])
                .agg(cnt=('id_x', 'nunique'))
                .reset_index()
            )

            actual = (
                recommendations[(recommendations['age'] >= 1) & recommendations['vaccination_date'].notna()]
                .groupby(['current_management_code', 'age', 'vaccination_name'])
                .agg(vac=('id_x', 'nunique'))
                .reset_index()
            )

            result = (
                actual.merge(expected, on=['current_management_code', 'age'])
                .assign(percent=lambda x: (x['vac'] / x['cnt'] * 100).round(2))
            )

        else:
            expected = (
                recommendations[recommendations['age'] >= 1]
                .groupby('current_management_code')
                .agg(cnt=('id_x', 'nunique'))
                .reset_index()
            )

            actual = (
                recommendations[(recommendations['age'] >= 1) & recommendations['vaccination_date'].notna()]
                .groupby(['current_management_code', 'vaccination_name'])
                .agg(vac=('id_x', 'nunique'))
                .reset_index()
            )

            result = (
                actual.merge(expected, on='current_management_code')
                .assign(percent=lambda x: (x['vac'] / x['cnt'] * 100).round(2))
            )

        return result
    
    def coverage(self):
        recommendations_df = self.analyze().assign(age=lambda x: x.age_month_x)
        
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
        last_day_of_month = pd.Timestamp(self.current_date.year, current_date.month, 1) + pd.offsets.MonthEnd(0)

        # 计算前12个月的第一天
        first_day_of_month = pd.Timestamp(self.current_date) - relativedelta(months=12, day=1)
        
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
    
person = pd.read_pickle('/mnt/d/标准库接种率/data/person2.pkl')
current_date = date(2021, 1, 31)
person=person.query("current_management_code=='334878619388'")
person['birth_date'] = pd.to_datetime(person['birth_date'], format='%Y%m%dT%H%M%S')
person['vaccination_date'] = pd.to_datetime(person['vaccination_date'], format='%Y%m%dT%H%M%S')
person['vaccination_org']=person['vaccination_org'].astype(str)
person['entry_org']=person['entry_org'].astype(str)
person['vaccination_code']=person['vaccination_code'].astype(str)
analyzer = VaccinationAnalyzer(person, current_date)
combined_result = analyzer.coverage()

