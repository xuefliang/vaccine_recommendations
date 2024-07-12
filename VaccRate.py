import pandas as pd 
import numpy as np
import janitor

@pd.api.extensions.register_dataframe_accessor("vaccine")
class VaccineAccessor:
    def __init__(self, df):
        self._validate(df)
        self._df = df

    @staticmethod
    def _validate(df):
        required_cols = ["id_x", "birth_date", "vaccination_code","vaccination_date"]
        if not all(col in df.columns for col in required_cols):
            raise ValueError("DataFrame does not contain all required columns.")
        
        if df['id_x'].dtype != 'object':
            raise ValueError("id_x column should be of type object.")
        if df['vaccination_code'].dtype != 'object':
            raise ValueError("vaccination_code column should be of type object.")
        if df['birth_date'].dtype != 'datetime64[ns]':
            raise ValueError("birth_date column should be of type datetime64[ns].")
        if df['vaccination_date'].dtype != 'datetime64[ns]':
            raise ValueError("vaccination_date column should be of type datetime64[ns].")
        
    def __getattr__(self, name):
        return getattr(self._df, name)
    
    # def age(self, tj_date):
    #     df = self._df
    #     df['age'] = np.floor((pd.to_datetime(tj_date, format='%Y-%m-%d') - df['birth_date']).dt.days / 365.25).astype(int)
    #     return df.query("age >= 1 & age <= 18")
    
    def _vaccine_filter(self, codes, name):
        df = self._df.copy()
        df = df.query("vaccination_code in @codes")
        df['vaccine_name'] = name
        df = df.groupby('id_x', group_keys=False).apply(
            lambda x: x.sort_values(by='vaccination_date', ascending=True)
            .assign(jc=range(1, len(x) + 1))
            .reset_index(drop=True)
        )
        return df

    def bcg(self):
        return self._vaccine_filter(['0101'], '卡介苗')

    def hbv(self):
        return self._vaccine_filter(['0201', '0202', '0203','2001'], '乙肝疫苗')
    
    def polio(self):
        return self._vaccine_filter(['0301', '0302', '0303','0304','0305','0306','0311','0312','5001'], '脊灰疫苗')
    
    def dpt(self):
        return self._vaccine_filter(['0401', '0402', '0403','4901','5001'], '百白破疫苗')
    
    def mcv(self):
        return self._vaccine_filter(['1201','1301','1401'],'含麻疹成分疫苗')
    
    def dtv(self):
        return self._vaccine_filter(['0601'],'白破疫苗')
    
    def mpva(self):
        return self._vaccine_filter(['1601'],'流脑疫苗A群')
    
    def mpvac(self):
        return self._vaccine_filter(['1701','1702','1703','1704','5301'],'流脑疫苗AC群')
    
    def jev(self):
        return self._vaccine_filter(['1801','1802','1803','1804'],'乙脑疫苗')
    
    def heva(self):
        return self._vaccine_filter(['1901','1902','1903','2001'],'甲肝疫苗')
    
    def all_vaccines(self):
        vaccines = [
            self.bcg(),
            self.hbv(),
            self.polio(),
            self.dpt(),
            self.mcv(),
            self.dtv(),
            self.mpva(),
            self.mpvac(),
            self.jev(),
            self.heva()
        ]
        return pd.concat(vaccines, ignore_index=True)


@pd.api.extensions.register_dataframe_accessor("analysis")
class AnalysisAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def calculate_vaccine_rate(self, vaccine_df):
        if not isinstance(vaccine_df, pd.DataFrame):
            raise ValueError("参数 'vaccine_df' 必须是一个 pandas DataFrame")

        required_columns = ['current_management_code', 'age', 'vaccine_name', 'jc', 'id_x']
        missing_columns = [col for col in required_columns if col not in vaccine_df.columns]
        if missing_columns:
            raise ValueError(f"'vaccine_df' DataFrame 缺少以下列: {missing_columns}")

        yingzhong = (
            self._obj
            .query("age >= 1")
            .groupby(['current_management_code', 'age'])
            .agg(cnt=('id_x', 'nunique'))
            .reset_index()
        )

        shizhong = (
            vaccine_df
            .query("age >= 1")
            .groupby(['current_management_code', 'age', 'vaccine_name', 'jc'])
            .agg(vac=('id_x', 'nunique'))
            .reset_index()
        )

        return shizhong.merge(yingzhong, on=['current_management_code', 'age'], how='inner').assign(
            percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
        ).reset_index()
    
@pd.api.extensions.register_dataframe_accessor("analysis")
class AnalysisAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def calculate_vaccine_rate(self, vaccine_df):
        if not isinstance(vaccine_df, pd.DataFrame):
            raise ValueError("参数 'vaccine_df' 必须是一个 pandas DataFrame")

        required_columns = ['current_management_code', 'age', 'vaccine_name', 'jc', 'id_x']
        missing_columns = [col for col in required_columns if col not in vaccine_df.columns]
        if missing_columns:
            raise ValueError(f"'vaccine_df' DataFrame 缺少以下列: {missing_columns}")

        yingzhong = (
            self._obj
            .query("age >= 1")
            .groupby(['current_management_code', 'age'])
            .agg(cnt=('id_x', 'nunique'))
            .reset_index()
        )

        shizhong = (
            vaccine_df
            .query("age >= 1")
            .groupby(['current_management_code', 'age', 'vaccine_name', 'jc'])
            .agg(vac=('id_x', 'nunique'))
            .reset_index()
        )

        result=shizhong.merge(yingzhong, on=['current_management_code', 'age'], how='inner').reset_index(drop=True).assign(
            percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
        ).reset_index()

        return result


person=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard.csv',dtype={'CURRENT_MANAGEMENT_CODE':str}).clean_names()
person__vaccination=pd.read_csv('/mnt/d/标准库接种率/标准库数据/person_standard_vaccination.csv',dtype={'VACCINATION_CODE':str}).clean_names()

person2=person.merge(person__vaccination,how='left',left_on='id',right_on='person_id')
person2['birth_date'] = pd.to_datetime(person2['birth_date'].astype(str).str.split('T').str[0], format='%Y%m%d') 
person2['vaccination_date']=pd.to_datetime(person2['vaccination_date'].astype(str).str.split('T').str[0], format='%Y%m%d') 
person2['age'] =np.floor((pd.to_datetime('2021-01-31', format='%Y-%m-%d') - person2['birth_date']).dt.days / 365.25).astype(int)

# tmp = person2.vaccine.all_vaccines()
# person2.analysis.calculate_vaccine_rate(tmp).to_excel('/mnt/d/标准库接种率/vacc_rate.xlsx',index=False)
