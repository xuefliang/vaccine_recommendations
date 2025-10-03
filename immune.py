import pandas as pd
from datetime import datetime
from typing import List
from dateutil.relativedelta import relativedelta
from scipy.stats import binomtest

class Immune:
    def __init__(self, df: pd.DataFrame, current_date: datetime):
        self._validate_dataframe(df)
        self.df = df.copy()
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

    def _validate_dataframe(self, df: pd.DataFrame):
        required_columns = {
            'id_x': object,
            'birth_date': 'datetime64[ns]',
            'current_management_code': object,
            'vaccination_date': 'datetime64[ns]',
            'vaccination_code': object,
            'vaccination_org': object,
            'entry_org': object
        }

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"缺少以下必需的列：{', '.join(missing_columns)}")

        for col, expected_type in required_columns.items():
            if df[col].dtype != expected_type:
                actual_type = df[col].dtype
                raise TypeError(f"列 '{col}' 的类型应为 {expected_type}，但实际为 {actual_type}")

    def _add_months(self,date:datetime, months:int):
        return date + relativedelta(months=months)

    def _get_hep_b_intervals(self, birth_date: datetime) -> List[int]:
        age_months=relativedelta(self.current_date, birth_date).months+relativedelta(self.current_date, birth_date).years*12
        if age_months < 12:
            return [0, 1, 5]  # 第3剂和第1剂间隔6个月，和第2剂隔1个月
        else:
            return [0, 1, 3]  # [0, 1, 3] 第3剂和第1剂间隔4个月，与第2剂次间隔不少于2个月

    def get_recommended_dates(self, vaccine_name: str, birth_date: datetime, vaccinations: List[datetime]) -> List[datetime]:
        if vaccine_name not in self.vaccines:
            return []

        vaccine_info = self.vaccines[vaccine_name]
        recommended_dates = []
        
        min_intervals = (vaccine_info["min_interval"](birth_date) 
                         if callable(vaccine_info["min_interval"]) 
                         else vaccine_info["min_interval"])

        last_date = birth_date
        schedule = vaccine_info["schedule"]

        for i in range(0, vaccine_info["doses"]):
            if i == 0:
                if not vaccinations or vaccinations[0] is None or (vaccinations[0] - birth_date).days < min_intervals[0] or vaccinations[0]>self.current_date:
                    # 第1剂次使用出生日期+最小间隔进行推算
                    recommended_date = self._add_months(birth_date, schedule[i])
                else:
                    recommended_date = None
            else:
                if i < len(vaccinations) and vaccinations[i] is not None and vaccinations[i] <self.current_date:
                    # 如果第i剂次已经接种,则不计算recommended_date
                    recommended_date = None
                elif i > 0 and i - 1 < len(vaccinations) and vaccinations[i-1] is not None:
                    # 如果上一次剂次已经接种,则使用上一针实际接种日期+最小间隔进行推算
                    last_date = vaccinations[i-1]
                    recommended_date = max(self._add_months(last_date, min_intervals[i]), self._add_months(birth_date, schedule[i]))
                else:
                    # 如果上一次剂次未接种,则置空
                    recommended_date = None
            
            recommended_dates.append(recommended_date)

        return recommended_dates

    def _process_vaccination_code(self):
        # 处理 vaccination_code 为 '5001' 的记录
        new_rows = []
        for index, row in self.df.iterrows():
            if row['vaccination_code'] == '5001':
                # 复制当前行并修改 vaccination_code
                new_row_1 = row.copy()
                new_row_1['vaccination_code'] = '0301'
                new_rows.append(new_row_1)

                new_row_2 = row.copy()
                new_row_2['vaccination_code'] = '0401'
                new_rows.append(new_row_2)

        # 将新行添加到 DataFrame
        self.df = pd.concat([self.df] + new_rows, ignore_index=True)

    ## 生成各类疫苗一下针的推荐时间
    def generate_recommendations(self) -> pd.DataFrame:
        # 新增调用处理方法
        self._process_vaccination_code()
        # 生成初始建议
        recommendations = self._generate_initial_recommendations()
        return recommendations

    def _generate_initial_recommendations(self) -> pd.DataFrame:
        recommendations = []
        self.df['vaccine_name'] = self.df['vaccination_code'].map(self.vaccine_code_map)

        # 预处理数据
        grouped_data = self.df.groupby('id_x')
        
        for id_x, individual_data in grouped_data:
            birth_date = individual_data['birth_date'].iloc[0]
            current_age_months = (self.current_date.year - birth_date.year) * 12 + (self.current_date.month - birth_date.month)
            latest_management_code = individual_data['current_management_code'].iloc[-1]

            for vaccine_name, vaccine_info in self.vaccines.items():
                vaccine_data = individual_data[individual_data['vaccine_name'] == vaccine_name]
                vaccine_entry_org = vaccine_data['entry_org'].iloc[0] if not vaccine_data.empty else None

                vaccinations = vaccine_data['vaccination_date'].tolist()
                vaccinations.sort()
                
                recommended_dates = self.get_recommended_dates(vaccine_name, birth_date, vaccinations)
                
                # 生成所有剂次的推荐数据
                for dose in range(1, vaccine_info['doses'] + 1):
                    recommended_date = recommended_dates[dose - 1]
                    if dose <= len(vaccinations):
                        date = vaccinations[dose - 1]
                        vacc_month = (date.year - birth_date.year) * 12 + (date.month - birth_date.month)
                        current_vaccination = individual_data[
                            (individual_data['vaccine_name'] == vaccine_name) & 
                            (individual_data['vaccination_date'] == date)
                        ]
                        vaccination_org = current_vaccination['vaccination_org'].iloc[0] if not current_vaccination.empty else None
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

        recommendations = pd.DataFrame(recommendations)
        
        # 更新满足条件的vaccination_org
        mask = (recommendations['vaccination_org'].isin(['777777777777', '888888888888', '999999999999'])) & \
               (recommendations['vaccine_name'].isin(['乙肝疫苗', '卡介苗'])) & \
               (recommendations['dose'] == 1)
        
        recommendations.loc[mask, 'vaccination_org'] = recommendations.loc[mask, 'entry_org']
        recommendations = recommendations.drop(columns=['entry_org'])
        return recommendations
    
    def get_first_dose_date(self, vaccine_name: str, birth_date: datetime) -> datetime:
        vaccine_info = self.vaccines[vaccine_name]
        min_intervals = (vaccine_info["min_interval"](birth_date) 
                         if callable(vaccine_info["min_interval"]) 
                         else vaccine_info["min_interval"])
        return self._add_months(birth_date,min_intervals[0])
    
    def other_vaccines(self) -> pd.DataFrame:
        if 'vaccine_name' not in self.df.columns:
            self.df['vaccine_name'] = self.df['vaccination_code'].map(self.vaccine_code_map)
        
        other_vaccines = self.df[~self.df['vaccine_name'].isin(self.vaccines.keys())]
        
        if other_vaccines.empty:
            return pd.DataFrame()  # 如果没有其他疫苗，返回空的DataFrame
        
        results = []
        for (id_x, vaccine_name), group in other_vaccines.groupby(['id_x', 'vaccine_name']):
            birth_date = group['birth_date'].iloc[0]
            current_age_months = (self.current_date.year - birth_date.year) * 12 + (self.current_date.month - birth_date.month)
            latest_management_code = group['current_management_code'].iloc[-1]
            vaccine_entry_org = group['entry_org'].iloc[0]

            # 按vaccination_date排序
            sorted_group = group.sort_values('vaccination_date')

            for i, row in enumerate(sorted_group.iterrows(), start=1):
                _, row_data = row
                vaccination_date = row_data['vaccination_date']
                vacc_month = (vaccination_date.year - birth_date.year) * 12 + (vaccination_date.month - birth_date.month)
                
                results.append({
                    'id_x': id_x,
                    'vaccine_name': vaccine_name,
                    'dose': i,  # 现在dose是按vaccination_date排序的
                    'vaccination_date': vaccination_date,
                    'recommended_date': None,
                    'current_management_code': latest_management_code,
                    'vaccination_org': row_data['vaccination_org'],
                    'vacc_month': vacc_month,
                    'age': current_age_months,
                    'entry_org': vaccine_entry_org,
                    'birth_date': birth_date
                })

        return pd.DataFrame(results)
    
    def process_vaccines(self):
        def process_group(group):
            # 处理流脑疫苗第一种情况
            condition1 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month < 6)
            if condition1.any():
                group.loc[condition1, 'vaccine_name'] = '流脑疫苗A群'
                group.loc[(group.vaccine_name == '流脑替代疫苗') & (group.dose == 3), 'vaccine_name'] = '流脑疫苗A群'
                group.loc[(group.vaccine_name == '流脑疫苗A群') & (group.dose == 3), 'dose'] = 2
            
            # 处理第二种情况
            condition2 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month >= 6) & (group.vacc_month < 24)
            if condition2.any():
                group.loc[group.vaccine_name == '流脑替代疫苗', 'vaccine_name'] = '流脑疫苗A群'
            
            # 处理第三种情况
            condition3 = (group.vaccine_name == '流脑替代疫苗') & (group.dose == 1) & (group.vacc_month >= 24)
            if condition3.any():
                group.loc[condition3, 'vaccine_name'] = '流脑疫苗AC群'
            
            # 处理第四种情况
            condition4 = (group.vaccine_name == '流脑替代疫苗') & (group.vacc_month >= 5*12)
            if condition4.any():
                group.loc[condition4, 'vaccine_name'] = '流脑疫苗AC群'
            
            # 新增：处理甲肝灭活疫苗的情况
            condition_hepA = (group.vaccine_name == '甲肝灭活疫苗') & (group.dose == 1)
            if condition_hepA.any():
                group.loc[condition_hepA, 'vaccine_name'] = '甲肝疫苗'
            
            # 新增：处理乙脑灭活疫苗的情况
            # 情况1：dose == 2 时
            condition_je1 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 2)
            if condition_je1.any():
                group.loc[condition_je1, 'vaccine_name'] = '乙脑疫苗'
                group.loc[condition_je1, 'dose'] = 1

            # 情况2：dose == 3 且 vacc_month < 6*12 时
            condition_je2 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 3) & (group.vacc_month < 6*12)
            if condition_je2.any():
                group.loc[condition_je2, 'vaccine_name'] = '乙脑疫苗'
                group.loc[condition_je2, 'dose'] = 2

            # 情况3：dose == 4 且 vacc_month >= 6*12 时
            condition_je3 = (group.vaccine_name == '乙脑灭活疫苗') & (group.dose == 4) & (group.vacc_month >= 6*12)
            if condition_je3.any():
                group.loc[condition_je3, 'vaccine_name'] = '乙脑疫苗'
                group.loc[condition_je3, 'dose'] = 2

            return group

        # 按 id_x 分组并应用处理函数
        processed_vaccines = self.other_vaccines().groupby('id_x').apply(process_group).reset_index(drop=True)
        return processed_vaccines
    
    def merge_recommendations(self) -> pd.DataFrame:
        # 获取推荐和其他疫苗的数据
        recommendations = self.generate_recommendations()
        other_vaccines = self.process_vaccines()

        # 合并数据
        merged = pd.merge(
            recommendations,
            other_vaccines[['id_x', 'vaccine_name', 'dose', 'vaccination_date', 'vaccination_org', 'vacc_month']],
            on=['id_x', 'vaccine_name', 'dose'],
            how='left',
            suffixes=('', '_other')
        )

        # 更新相应的列
        columns_to_update = ['vaccination_date', 'vaccination_org', 'vacc_month']
        for col in columns_to_update:
            merged[col] = merged[f'{col}_other'].fillna(merged[col])
            merged = merged.drop(columns=[f'{col}_other'])

        merged=self._update_ac_recommendations(merged)
        merged=self._update_recommendations(merged)
        return merged
    
    def _update_ac_recommendations(self, recommendations_df: pd.DataFrame) -> pd.DataFrame:
        grouped = recommendations_df.groupby('id_x')
        updated_recommendations = []
        
        for _, group in grouped:
            birth_date = group['birth_date'].iloc[0]
            current_age_months = group['age'].iloc[0]
            
            # 获取流脑疫苗A群和AC群的数据
            a_group = group[group['vaccine_name'] == '流脑疫苗A群']
            ac_group = group[group['vaccine_name'] == '流脑疫苗AC群']
            
            # 检查流脑疫苗A群的第1剂和第2剂是否都已接种
            a_group_doses_completed = (a_group['dose'].isin([1, 2]) & a_group['vaccination_date'].notna()).all()
            a_group_dose1_completed = (a_group['dose'] == 1).any() and (a_group[a_group['dose'] == 1]['vaccination_date'].notna().any())
            
            # 新增：检查流脑疫苗A群接种2针的时的年龄是否均小于2*12
            a_group_doses_under_24_months = (a_group['vacc_month'] < 24).all()
            
            # 处理流脑疫苗AC群的接种日期
            for idx, row in ac_group.iterrows():
                if row['dose'] == 1:
                    if current_age_months >= 3*12 and current_age_months < 6*12 and a_group_doses_completed:
                        ac_group.at[idx, 'recommended_date'] = self._add_months(birth_date, 36)
                    elif current_age_months >= 2*12 and current_age_months < 3*12 and a_group_doses_completed:
                        # 新增：当A群接种2针且年龄在24-36个月之间时，不计算recommended_date
                        ac_group.at[idx, 'recommended_date'] = None
                    elif a_group_doses_under_24_months:
                        ac_group.at[idx, 'recommended_date'] = None
                    elif current_age_months >= 2*12 and a_group_dose1_completed:
                        a_group_dose2 = a_group[a_group['dose'] == 2]
                        if not a_group_dose2.empty and pd.notna(a_group_dose2['vaccination_date'].iloc[0]):
                            ac_group.at[idx, 'recommended_date'] = self._add_months(a_group_dose2['vaccination_date'].iloc[0], 3)
                        else:
                            ac_group.at[idx, 'recommended_date'] = self._add_months(birth_date, 24)
                    elif current_age_months >= 2*12 and not a_group_doses_completed:
                        ac_group.at[idx, 'recommended_date'] = self._add_months(birth_date, 24)

                elif row['dose'] == 2:
                    ac_dose_1 = ac_group[ac_group['dose'] == 1]
                    if not ac_dose_1.empty and ac_dose_1['vaccination_date'].notna().any():
                        vaccination_date = ac_dose_1['vaccination_date'].iloc[0]
                        if pd.notna(vaccination_date):
                            ac_group.at[idx, 'recommended_date'] = self._add_months(vaccination_date, 36)
            
            # 更新组内的AC群数据
            group.loc[group['vaccine_name'] == '流脑疫苗AC群'] = ac_group
            
            updated_recommendations.append(group)
        
        return pd.concat(updated_recommendations, ignore_index=True)
    
    def _update_recommendations(self, recommendations_df: pd.DataFrame) -> pd.DataFrame:
        # 遍历每一行数据
        for idx, row in recommendations_df.iterrows():
            # 检查vaccination_date是否current_date之前
            if pd.notna(row['vaccination_date']) and row['vaccination_date'] < self.current_date:
                # 将recommended_date置空
                recommendations_df.at[idx, 'recommended_date'] = None
        return recommendations_df

    @staticmethod
    def calculate_ci(k, n, confidence_level=0.95):
        bino = binomtest(k=k, n=n, p=0.05, alternative='two-sided')
        ci = bino.proportion_ci(confidence_level=confidence_level)
        return pd.Series([ci.low * 100, ci.high * 100], index=['lower_ci', 'upper_ci'])

    def cohort_rate(self, by_age=False, calculate_ci=False):
        recommendations = self.merge_recommendations()
        recommendations['age'] = recommendations['age'] // 12

        if by_age:
            expected = (
                recommendations
                .query("age >= 1")
                .groupby(['current_management_code', 'age'])
                .agg(cnt=('id_x', 'nunique'))
                .reset_index()
            )

            actual = (
                recommendations
                .query("age >= 1 and vaccination_date.notna()")  # 添加过滤条件
                .groupby(['current_management_code', 'age', 'vaccine_name', 'dose'])
                .agg(vac=('id_x', 'nunique'))
                .reset_index()
            )

            result = actual.merge(expected, on=['current_management_code', 'age'], how='inner').reset_index(drop=True).assign(
                percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
            ).reset_index()

        else:
            expected = (
                recommendations
                .query("age >= 1")
                .groupby(['current_management_code'])
                .agg(cnt=('id_x', 'nunique'))
                .reset_index()
            )

            actual = (
                recommendations
                .query("age >= 1 and vaccination_date.notna()")  # 添加过滤条件
                .groupby(['current_management_code', 'vaccine_name', 'dose'])
                .agg(vac=('id_x', 'nunique'))
                .reset_index()
            )

            result = actual.merge(expected, on=['current_management_code'], how='inner').reset_index(drop=True).assign(
                percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
            ).reset_index()

        if calculate_ci:
            result[['lower_ci', 'upper_ci']] = result.apply(
                lambda row: self.calculate_ci(int(row['vac']), int(row['cnt']), confidence_level=0.95), axis=1
            )
        return result
    
    def generate_coverage(self, calculate_ci=False) -> pd.DataFrame:
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
                (recommendations_df['vaccination_date'].dt.year == self.current_date.year) &
                (recommendations_df['vaccination_date'].dt.month == self.current_date.month) &
                (recommendations_df['age'] >= condition['actual_min_age']) &
                (recommendations_df['age'] < condition['actual_max_age']) &
                (recommendations_df['vaccine_name'].isin(condition['vaccines'])) &
                (recommendations_df['dose'].isin(condition['doses'])) &
                (recommendations_df['vaccination_date'].notna()) &
                (recommendations_df['vaccination_org']==recommendations_df['current_management_code'])
            )
            
            actual = (
                recommendations_df[mask_actual]
                .groupby(['vaccine_name', 'vaccination_org', 'dose'])
                .agg(actual_count=('id_x', 'nunique'))
                .reset_index()
            )
            
            # 计算应种数
            mask_expected = (
                (recommendations_df['recommended_date'].dt.year * 12 + recommendations_df['recommended_date'].dt.month) >= 
                ((self.current_date - relativedelta(months=12)).year * 12 + (self.current_date - relativedelta(months=12)).month)
            ) & (
                (recommendations_df['recommended_date'].dt.year * 12 + recommendations_df['recommended_date'].dt.month) <= 
                (self.current_date.year * 12 + self.current_date.month)
            ) & (
                (recommendations_df['age'] >= condition['expected_min_age']) & 
                (recommendations_df['age'] < condition['expected_max_age']) &
                (recommendations_df['vaccine_name'].isin(condition['vaccines'])) &
                (recommendations_df['dose'].isin(condition['doses']))
            )
            
            expected = (
                recommendations_df[mask_expected]
                .groupby(['vaccine_name', 'current_management_code', 'dose'])
                .agg(expected_count=('id_x', 'nunique'))
                .reset_index()
            )
            
            # 合并实种数和应种数，计算接种率
            result = (
                expected
                .merge(actual, 
                       left_on=['vaccine_name', 'current_management_code', 'dose'],
                       right_on=['vaccine_name', 'vaccination_org', 'dose'],
                       how='left')
                .fillna(0)
                .assign(
                    prop=lambda x: (x['actual_count'] / (x['expected_count'] + x['actual_count']) * 100).round(2)
                )
            )
            
            coverage_data.append(result)
        
        # 合并所有结果
        cover = pd.concat(coverage_data, ignore_index=True)
        if calculate_ci:
            cover[['lower_ci', 'upper_ci']] = cover.apply(
                lambda row: self.calculate_ci(int(row['actual_count']), int(row['actual_count'] + row['expected_count']), confidence_level=0.95), axis=1
            )
        return cover


import sys
import os
import pandas as pd
import numpy as np
from datetime import timedelta
import janitor


# person2=pd.read_pickle('/mnt/d/标准库接种率/data/person2.pkl')
# person2=person2.query("current_management_code=='334878619388' | vaccination_org=='334878619388'")
# person2['birth_date'] = pd.to_datetime(person2['birth_date'], format='%Y%m%dT%H%M%S')
# person2['vaccination_date'] = pd.to_datetime(person2['vaccination_date'], format='%Y%m%dT%H%M%S')
# person2['vaccination_org']=person2['vaccination_org'].astype(str)
# person2['entry_org']=person2['entry_org'].astype(str)

current_date = datetime(2021, 1, 31)
immune = Immune(person2, current_date)
recommendations=immune.merge_recommendations()
cover=immune.generate_coverage(calculate_ci=True).query("current_management_code=='334878619388'")

current_date = datetime(2021, 12, 31)
immune = Immune(person2, current_date)
cohort_rate=immune.cohort_rate(by_age=True,calculate_ci=True).query("current_management_code=='334878619388'")