import pandas as pd
from script.common import *

@pd.api.extensions.register_dataframe_accessor("vaccine")
class VaccineAccessor:

    def __init__(self, df):
        self._validate(df)
        self._df = df
        self.jzjl = self.set_jzjl()
        self._update_vaccination_org()
        self.mon_end = None
        self.jzjc = self.get_immuhis()

    @staticmethod
    def _validate(df):
        required_cols = ["id_x", "birth_date", "vaccination_code", "vaccination_date",
                         "vaccination_org","entry_org","current_management_code"]
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
        if df['vaccination_org'].dtype != 'object':
            raise ValueError("vaccination_org column should be of type object.")
        if df['entry_org'].dtype != 'object':
            raise ValueError("entry_org column should be of type object.")
        if df['current_management_code'].dtype != 'object':
            raise ValueError("current_management_code column should be of type object.")
    
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
        return self._vaccine_filter(['0201', '0202', '0203', '2001'], '乙肝疫苗')
    
    def polio(self):
        return self._vaccine_filter(['0301', '0302', '0303', '0304', '0305', '0306', '0311', '0312', '5001'], '脊灰疫苗')
    
    def dpt(self):
        return self._vaccine_filter(['0401', '0402', '0403', '4901', '5001'], '百白破疫苗')
    
    def mcv(self):
        return self._vaccine_filter(['1201', '1301', '1401'], '含麻疹成分疫苗')
    
    def dtv(self):
        return self._vaccine_filter(['0601'], '白破疫苗')
    
    def mpva(self):
        return self._vaccine_filter(['1601'], '流脑疫苗A群')
    
    def mpvac(self):
        return self._vaccine_filter(['1701', '1702', '1703', '1704', '5301'], '流脑疫苗AC群')
    
    def jev(self):
        return self._vaccine_filter(['1801', '1802', '1803', '1804'], '乙脑疫苗')
    
    def heva(self):
        return self._vaccine_filter(['1901', '1902', '1903', '2001'], '甲肝疫苗')
    
    def hbvj(self):
        hbv_df = self.hbv()
        mask = (hbv_df['jc'] == 1) & ((hbv_df['vaccination_date'] - hbv_df['birth_date']) <= timedelta(hours=24))
        hbv_df.loc[mask, 'vaccine_name'] = '乙肝疫苗1及时'
        return hbv_df[hbv_df['vaccine_name'] == '乙肝疫苗1及时']

    def set_mon_end(self, mon_stat):
        self.mon_end = pd.date_range(mon_stat, periods=1, freq='M')[0]

    def set_jzjl(self):
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

    def get_immuhis(self):
        vaccine_names = ['卡介苗', '乙肝疫苗', '脊灰疫苗', '百白破疫苗', '含麻疹成分疫苗', '白破疫苗', '流脑疫苗A群', '流脑疫苗AC群', '乙脑疫苗', '甲肝疫苗']
        
        jzjc = (
            pd.MultiIndex.from_product(
                [self.jzjl['id_x'].unique(), vaccine_names],
                names=['id_x', 'vaccine_name']
            )
            .to_frame(index=False)
            .merge(
                self.jzjl.groupby(['id_x','vaccine_name']).agg(jzjc=('id_x', 'size')).reset_index(),
                on=['id_x', 'vaccine_name'],
                how='left'
            )
            .fillna(0)
            .astype({'jzjc': 'int'})
        )
        return jzjc

    def BCG(self, mon_stat):
        # 卡介苗1实种统计
        actual = (
            self.jzjl.query("age < 4 & vaccine_name == '卡介苗' & year_month == @mon_stat & jc == 1")
            .groupby('vaccination_org')
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 筛选出乙肝疫苗且 jzjc 为 0 的记录的 id_x
        id_0 = self.jzjc.query('vaccine_name == "卡介苗" & jzjc == 0')['id_x'].unique()
        
        # 卡介苗1应种统计
        expected = (
            self._df.query("id_x in @id_0 & age < 4 & age >= 0")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby('current_management_code')
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种统计
        invalid = (
            self.jzjl.query("vaccine_name == '卡介苗' & jc == 1 & year_month > @mon_stat & age < 4 & age >= 0")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] >= self.mon_end]
            .groupby('current_management_code')
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 合并数据框并计算比例
        result = calculate_vaccine_proportion(expected, actual, invalid, '卡介苗1')
        return result
    
    def HBV1(self, mon_stat):
        # 乙肝1实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='乙肝疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙肝1应种
        # 筛选出乙肝疫苗且 jzjc 为 0 的记录
        id_0=self.jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 0')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_0 & age <= 6 & age >= 0")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种统计
        invalid = (
            self.jzjl
            .query("vaccine_name == '乙肝疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & age >= 0")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] >= self.mon_end]
            .groupby('current_management_code')
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 合并数据框并计算比例
        result = calculate_vaccine_proportion(expected, actual, invalid, '乙肝疫苗1')
        return result

    def HBV2(self,  mon_stat):
        # 乙肝2实种
        actual = (
            self.jzjl
            .query("age < 18 & vaccine_name == '乙肝疫苗' & year_month == @mon_stat & jc == 2")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙肝2应种
        # 筛选出乙肝疫苗且 jzjc 为 1 的记录
        id_1 = self.jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 1')['id_x'].unique()
        expected = (
            self.jzjl
            .query("id_x in @id_1 & age <= 6 & age >= 0 & vaccine_name == '乙肝疫苗'")
            .query("year_month < @mon_stat")
            .pipe(add_13_months, 'vaccination_date')
            .loc[lambda df: df['plus_13_months'] >= pd.to_datetime(self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        id_2 = self.jzjl.query("vaccine_name == '乙肝疫苗' & jc == 2 & year_month > @mon_stat & age <= 6 & age >= 0")['id_x'].unique()

        invalid = (
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '乙肝疫苗' & jc <= 2")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] >= pd.to_datetime(self.mon_end)]
            .groupby('id_x')
            .apply(intervals, mon_stat=mon_stat)
            .reset_index(drop=True)
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 合并数据框并计算比例
        result = calculate_vaccine_proportion(expected, actual, invalid, '乙肝疫苗2')
        return result

    def HBV3(self,  mon_stat):
    # 乙肝3实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='乙肝疫苗' & year_month==@mon_stat & jc==3")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙肝3应种
        # 筛选出乙肝疫苗且 jzjc 为 2 的记录
        id_1 = self.jzjc.query('vaccine_name == "乙肝疫苗" & jzjc == 2')['id_x'].unique()
        expected = (
            self.jzjl
            .query("id_x in @id_1 & age <= 6 & age >= 0 & vaccine_name == '乙肝疫苗' & jc == 2")
            .pipe(add_intervals, 'vaccination_date', 5)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: df['add_intervals'] <= pd.to_datetime(self.mon_end)]
            .loc[lambda df: df['plus_13_months'] >= pd.to_datetime(self.mon_end)]
            .groupby('current_management_code')
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙肝3已种，但不符合要求
        id_2=self.jzjl.query("vaccine_name == '乙肝疫苗' & jc == 3 & year_month > @mon_stat & age <= 6 & age >= 0")['id_x'].unique()

        # .loc[mon_end < jzjl['birth_date'] + pd.DateOffset(months=13)] 上针13剂次
        id_2 = self.jzjl.query("vaccine_name == '乙肝疫苗' & jc == 3 & year_month > @mon_stat & age <= 6 & age >= 0")['id_x'].unique()
        invalid = (
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '乙肝疫苗' & jc <= 3")
            .pipe(add_13_months, 'birth_date')
            .loc[lambda df: df['plus_13_months'] >= pd.to_datetime(self.mon_end)]
            .groupby('id_x')
            .apply(intervals, mon_stat=mon_stat)
            .reset_index(drop=True)
            .groupby('current_management_code')
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 合并数据框并计算比例
        result = calculate_vaccine_proportion(expected, actual, invalid, '乙肝疫苗3')
        return result

    def DPT1(self, mon_stat):
        # 百白破1实种
        actual=(
            self.jzjl
            .query("age<6 & vaccine_name=='百白破疫苗' & year_month== @mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 百白破1应种
        # 筛选出百白破1疫苗且 jzjc 为 0 的记录
        id_1=self.jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 0')['id_x']
        expected=(
            self._df
            .query("id_x in @id_1 & age <= 5 & months>=3")
            .pipe(add_intervals, 'birth_date', 3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        id_2=self.jzjl.query("vaccine_name == '百白破疫苗' & jc == 1 & year_month > @mon_stat & age <= 5 & months>=3")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 1")
            .pipe(add_intervals, 'birth_date', 3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '百白破疫苗1')
        return result

    def DPT2(self,  mon_stat):
        # 百白破2实种
        actual=(
            self.jzjl
            .query("age<6 & vaccine_name=='百白破疫苗' & year_month==@mon_stat & jc==2")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 百白破2应种
        # 筛选出百白破疫苗且 jzjc 为 1 的记录
        id_1=self.jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 1')['id_x'].unique()
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <=5 & months>=4 & vaccine_name=='百白破疫苗' & year_month<@mon_stat & jc==1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的2剂次接种
        id_2=self.jzjl.query("vaccine_name == '百白破疫苗' & jc == 2 & year_month > @mon_stat & age <=5 & months>=4")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '百白破疫苗2')
        return result

    def DPT3(self,  mon_stat):
        # 百白破3实种
        actual=(
            self.jzjl
            .query("age<6 & vaccine_name=='百白破疫苗' & year_month==@mon_stat & jc==3")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 百白破3应种
        # 筛选出百白破疫苗且 jzjc 为 2 的记录
        id_1=self.jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 2')['id_x']
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <= 5 & months>=5 & vaccine_name=='百白破疫苗' & year_month<@mon_stat & jc == 2")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的3剂次接种
        id_2=self.jzjl.query("vaccine_name == '百白破疫苗' & jc == 3 & year_month > @mon_stat & age <= 5 & months>=5")['id_x']

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 2")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '百白破疫苗3')
        return result

    def DPT4(self,  mon_stat):
        # 百白破4实种
        actual=(
            self.jzjl
            .query("age<6 & vaccine_name=='百白破疫苗' & year_month==@mon_stat & jc==4")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 百白破4应种
        # 筛选出百白破疫苗且 jzjc 为 3 的记录
        id_1=self.jzjc.query('vaccine_name == "百白破疫苗" & jzjc == 3')['id_x']
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age < 6 & months>=18 & vaccine_name=='百白破疫苗' & year_month<@mon_stat & jc == 3")
            .pipe(add_intervals, 'vaccination_date', 6)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的4剂次接种，4岁应种
        id_2=self.jzjl.query("vaccine_name == '百白破疫苗' & jc == 4 & year_month > @mon_stat & age < 6 & months>=18")['id_x']

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '百白破疫苗' & jc == 3")
            .pipe(add_intervals, 'vaccination_date', 6)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '百白破疫苗4')
        return result

    def HepA(self,  mon_stat):
        # 甲肝1实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='甲肝疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 甲肝1应种
        # 筛选出甲肝1疫苗且 jzjc 为 0 的记录
        id_1=self.jzjc.query('vaccine_name == "甲肝疫苗" & jzjc == 0')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_1 & age <= 6 & months>=18")
            .pipe(add_intervals, 'birth_date', 18)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

    # 不符合要求的1剂次接种
        id_2=self.jzjl.query("vaccine_name == '甲肝疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & months>=18")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '甲肝疫苗' & jc == 1")
            .pipe(add_intervals, 'birth_date', 18)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 合并数据框并计算比例
        result = calculate_vaccine_proportion(expected, actual, invalid, '甲肝疫苗1')
        return result

    def JEV1(self,  mon_stat):
        # 乙脑1实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='乙脑疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙脑1应种
        # 筛选出乙脑1疫苗且 jzjc 为 0 的记录
        id_1=self.jzjc.query('vaccine_name == "乙脑疫苗" & jzjc == 0')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_1 & age <= 6 & months>8")
            .pipe(add_13_months, 'birth_date',8)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        id_2=self.jzjl.query("vaccine_name == '乙脑疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & months>=8")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '乙脑疫苗' & jc == 1")
            .pipe(add_13_months, 'birth_date',8)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )
        result = calculate_vaccine_proportion(expected, actual, invalid, '乙脑疫苗1')
        return result

    def JEV2(self,  mon_stat):
        # 乙脑2实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='乙脑疫苗' & year_month==@mon_stat & jc==2")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 乙脑2应种
        # 筛选出乙脑疫苗且 jzjc 为 1 的记录
        id_1=self.jzjc.query('vaccine_name == "乙脑疫苗" & jzjc == 1')['id_x']
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <=6 & age>=2 & vaccine_name=='乙脑疫苗' & year_month<@mon_stat & jc==1")
            .pipe(add_intervals, 'vaccination_date', 12)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的2剂次接种
        id_2=self.jzjl.query("vaccine_name == '乙脑疫苗' & jc == 2 & year_month > @mon_stat & age <=6 & age>=2")['id_x']

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '乙脑疫苗' & jc == 1")
            .pipe(add_intervals, 'vaccination_date', 12)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '乙脑疫苗2')
        return result

    def MCV1(self,  mon_stat):
    # 含麻疹成分1实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='含麻疹成分疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 含麻疹成分1应种
        # 筛选出含麻疹成分1疫苗且 jzjc 为 0 的记录
        id_1=self.jzjc.query('vaccine_name == "含麻疹成分疫苗" & jzjc == 0')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_1 & age <= 6 & months>=8")
            .pipe(add_13_months, 'birth_date',8)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        id_2=self.jzjl.query("vaccine_name == '含麻疹成分疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & months>=8")['id_x']

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '含麻疹成分疫苗' & jc == 1")
            .pipe(add_13_months, 'birth_date',8)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '含麻疹成分疫苗1')
        return result

    def MCV2(self,  mon_stat):
        # 含麻疹成分2实种
        actual=(
            self.jzjl
            .query("age<18 & vaccine_name=='含麻疹成分疫苗' & year_month==@mon_stat & jc==2")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 含麻疹成分2应种
        # 筛选出含麻疹成分疫苗且 jzjc 为 1 的记录
        id_1=self.jzjc.query('vaccine_name == "含麻疹成分疫苗" & jzjc == 1')['id_x'].unique()
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <=6 & months>=18 & vaccine_name=='含麻疹成分疫苗' & year_month<@mon_stat & jc==1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的2剂次接种
        id_2=self.jzjl.query("vaccine_name == '含麻疹成分疫苗' & jc == 2 & year_month > @mon_stat & age <=6 & months>=18")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '含麻疹成分疫苗' & jc == 1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '含麻疹成分疫苗2')
        return result

    def PV1(self,  mon_stat):
        # 脊灰1实种
        actual=(
            self.jzjl
            .query("age<=18 & vaccine_name=='脊灰疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 脊灰1应种
        # 筛选出脊灰疫苗且 jzjc 为 0 的记录
        id_0=self.jzjc.query('vaccine_name == "脊灰疫苗" & jzjc == 0')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_0 & age <= 6 & months>=2")
            .pipe(add_13_months, 'birth_date', 3)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        invalid=(
            self.jzjl
            .query("vaccine_name == '脊灰疫苗' & jc == 1 & year_month > @mon_stat & age <= 6 & months>=2")
            .pipe(add_13_months, 'birth_date',3)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '脊灰疫苗1')
        return result

    def PV2(self,  mon_stat):
        # 脊灰2实种
        actual=(
            self.jzjl
            .query("age<=18 & vaccine_name=='脊灰疫苗' & year_month==@mon_stat & jc==2")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 脊灰2应种
        # 筛选出脊灰疫苗且 jzjc 为 1 的记录
        id_1=self.jzjc.query('vaccine_name == "脊灰疫苗" & jzjc == 1')['id_x'].unique()
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <= 6 & months>=3 & vaccine_name=='脊灰疫苗' & year_month<@mon_stat & jc==1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的2剂次接种
        id_2=self.jzjl.query("vaccine_name == '脊灰疫苗' & jc == 2 & year_month > @mon_stat & age <= 6 & months>=3")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '脊灰疫苗' & jc == 1")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )
        result = calculate_vaccine_proportion(expected, actual, invalid, '脊灰疫苗2')
        return result

    def PV3(self,  mon_stat):
        # 脊灰3实种
        actual=(
            self.jzjl
            .query("age<=18 & vaccine_name=='脊灰疫苗' & year_month==@mon_stat & jc==3")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 脊灰3应种
        # 筛选出脊灰疫苗且 jzjc 为 2 的记录
        id_1=self.jzjc.query('vaccine_name == "脊灰疫苗" & jzjc == 2')['id_x'].unique()
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <= 6 & months>=4 & vaccine_name=='脊灰疫苗' & year_month<@mon_stat & jc == 2")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的3剂次接种
        id_2=self.jzjl.query("vaccine_name == '脊灰疫苗' & jc == 3 & year_month > @mon_stat & age <= 6 & months>=4")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '脊灰疫苗' & jc == 2")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '脊灰疫苗3')
        return result

    def PV4(self,  mon_stat):
        # 脊灰4实种
        actual=(
        self.jzjl
        .query("age<=18 & vaccine_name=='脊灰疫苗' & year_month==@mon_stat & jc==4")
        .groupby(['vaccination_org'])
        .agg(actual_count=('id_x', 'nunique'))
        .reset_index()
        )

        # 脊灰4应种
        # 筛选出脊灰疫苗且 jzjc 为 3 的记录
        id_1=self.jzjc.query('vaccine_name == "脊灰疫苗" & jzjc == 3')['id_x'].unique()
        expected=(
            self.jzjl
            .query("id_x in @id_1 & age <= 6 & age>=4 & vaccine_name=='脊灰疫苗' & year_month<@mon_stat & jc == 3")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的4剂次接种，4岁应种
        id_2=self.jzjl.query("vaccine_name == '脊灰疫苗' & jc == 4 & year_month > @mon_stat & age <= 6 & age>=4")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '脊灰疫苗' & jc == 3")
            .pipe(add_intervals, 'vaccination_date', 1)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '脊灰疫苗4')
        return result

    def MenA1(self,  mon_stat):
        # 流脑A1实种
        actual=(
            self.jzjl
            .query("(age<2 & vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==1) | (age<2 & vaccine_name=='流脑疫苗A群' & year_month==@mon_stat & jc==1)")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 流脑A1应种
        # 筛选出流脑A1疫苗且 jzjc 为 0 的记录,需要增加疫苗编码1702的排除条件。
        id_1=self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 0')['id_x'].unique()
        mena_id=self.jzjl.query('vaccine_name == "流脑疫苗AC群" & jc == 1')['id_x'].unique()
        expected=(
            self._df
            .query("id_x in @id_1 & months <=23 & months>=6 & id_x not in@mena_id")
            .pipe(add_13_months, 'birth_date',6)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        id_2=self.jzjl.query("vaccine_name == '流脑疫苗A群' & jc == 1 & year_month > @mon_stat & months <=23 & months>=6")['id_x']
        id_3=self.jzjl.query("vaccine_name == '流脑疫苗AC群' & jc == 1 & year_month > @mon_stat & months <=23 & months>=6")['id_x']

        invalid=(
            self.jzjl
            .query("(id_x in @id_2 & vaccine_name == '流脑疫苗A群' & jc == 1) | (id_x in @id_3 & vaccine_name == '流脑疫苗AC群' & jc == 1)")
            .pipe(add_13_months, 'birth_date',6)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '流脑疫苗A群1')
        return result
    
    def MenA2(self,  mon_stat):
        actual=(
            self.jzjl
            .query("(age<2 & vaccine_name=='流脑疫苗A群' & year_month==@mon_stat & jc==2) | (age<2 & vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==2)")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 流脑A1应种
        # 筛选出流脑A1疫苗且 jzjc 为 0 的记录,需要增加疫苗编码1702的排除条件。
        id_1=self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x'].unique()
        mena_id=self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1') ['id_x'].unique()
        mena_id=self.jzjl.query(" id_x in @mena_id & months<=23")['id_x'].unique()
        expected=(
            self.jzjl
            .query("(id_x in @id_1 & months <=23 & months>=9 & year_month<@mon_stat & jc==1 & vaccine_name =='流脑疫苗A群')|(id_x in @mena_id & months <=23 & months>=9 & year_month<@mon_stat & vaccine_name =='流脑疫苗AC群' & jc==1)")
            .pipe(add_intervals, 'vaccination_date', 3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的2剂次接种
        id_2=self.jzjl.query("vaccine_name == '流脑疫苗A群' & jc == 2 & year_month > @mon_stat & months <=23 & months>=9")['id_x'].unique()
        id_3=self.jzjl.query("vaccine_name == '流脑疫苗AC群' & jc == 2 & year_month > @mon_stat & months <=23 & months>=9")['id_x'].unique()

        invalid=(
            self.jzjl
            .query("(id_x in @id_2 & vaccine_name == '流脑疫苗A群' & jc == 1) | (id_x in @id_3 & vaccine_name == '流脑疫苗AC群' & jc == 1)")
            .pipe(add_intervals, 'vaccination_date', 3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '流脑疫苗A群2')
        return result

    def MenAC1(self,  mon_stat):
        # 流脑AC1实种
        actual = (
            self.jzjl
            .query("(vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==1 & age>=2 & age<18)")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 流脑AC1应种
        # 1. 3岁-6岁且已接种A群流脑多糖疫苗第1、2剂，但未接种A群C群流脑多糖疫苗第1剂的儿童数,
        # 1702 等2岁之前接种替代疫苗按未种，
        # id_ac为A群已种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 2')['id_x']
        id_ac = self.jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 2')['id_x']

        # A群已种
        id_2 = pd.concat([id_ac, id_a], ignore_index=True)

        # id_ac2 为AC群未种
        id_ac2 = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']

        id_acs = self.jzjl.query("vaccination_code in ['1701','1702','1703','1704','5301']")['id_x']
        id_acw = self.jzjl.query("id_x not in @id_acs & age>=3")['id_x']
        id_ac2 = pd.concat([id_ac2, id_acw], ignore_index=True)

        id_3 = self.jzjl.query('id_x in @id_ac2 & id_x in @id_2')['id_x']
        menac1_subset1 = self.jzjl.query("id_x in @id_3 & age <= 6 & age >= 3").assign(
            vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 3)
        ).loc[lambda df: (df['vaccination_date_plus_13_months'] > self.mon_end)]

        # 2. ≥2岁且仅接种A群流脑多糖疫苗第1剂的儿童数（间隔3个月）。
        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x']
        id_a = self.jzjl.query("id_x in @id_a & vacc_months<=24")['id_x']

        # id_ac为A群已种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
        id_ac = self.jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

        # A群已种
        id_2 = pd.concat([id_a, id_ac], ignore_index=True)

        # id_ac2 为AC群小于2岁接种了一针和AC群接种0
        id_ac2 = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']
        id_acs = self.jzjl.query("vaccination_code in ['1701','1702','1703','1704','5301']")['id_x']
        id_acw = self.jzjl.query("id_x not in @id_acs & age>=2")['id_x']
        id_ac2 = pd.concat([id_ac2, id_acw], ignore_index=True)

        id_3 = self.jzjl.query('id_x in @id_ac2 & id_x in @id_2')['id_x']
        menac1_subset2 = self.jzjl.query("id_x in @id_3 & age >= 2 & vaccine_name in ['流脑疫苗A群','流脑疫苗AC群'] ").assign(
            vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
            vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 13 * 2)
        ).loc[lambda df: (df['vaccination_date_plus_interval'] <= self.mon_end) & (df['vaccination_date_plus_13_months'] > self.mon_end)]

        # 3. ≥2岁且未接种A群流脑多糖疫苗的儿童数。 
        # id_ac为A群未种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 0')['id_x']

        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 0')['id_x']

        id_3 = self.jzjl.query('id_x in @id_ac & id_x in @id_a')['id_x']
        menac1_subset3 = self.jzjl.query("id_x in @id_3 & age >= 2").assign(
            vaccination_date_plus_interval=lambda df: df['birth_date'] + pd.DateOffset(months=12 * 2),
            vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 2)
        ).loc[lambda df: (df['vaccination_date_plus_interval'] <= self.mon_end) & (df['vaccination_date_plus_13_months'] > self.mon_end)]

        expected = (
            pd.concat([menac1_subset1, menac1_subset2, menac1_subset3], ignore_index=True)
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        # 流脑AC1应种
        # 1. 3岁-6岁且已接种A群流脑多糖疫苗第1、2剂，但已接种A群C群流脑多糖疫苗第1剂的儿童数,
        # id_ac为A群已种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 2')['id_x']
        id_ac = self.jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 2')['id_x']

        # A群已种
        id_2 = pd.concat([id_ac, id_a], ignore_index=True)

        # id_ac2 为AC群已种
        id_ac2 = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
        id_acs = self.jzjl.query("id_x in @id_ac2 & age>=3")['id_x']
        id_3 = self.jzjl.query('id_x in @id_acs & id_x in @id_2')['id_x']

        menac1_subset1 = (
            self.jzjl
            .query("id_x in @id_3 & vaccine_name == '流脑疫苗AC群' & jc == 1 & age<=6")
            .assign(
                vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 3 * 12)
            )
            .loc[lambda df: ((df['vaccination_date'] > df['vaccination_date_plus_13_months']) & (df['vaccination_date'] > self.mon_end))]
        )

        # 2. ≥2岁且已接种A群流脑多糖疫苗第1剂，并在间隔3个月内接种了AC群流脑多糖疫苗第1剂的儿童数。
        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 1')['id_x']
        id_a = self.jzjl.query("id_x in @id_a & vacc_months<=24")['id_x']

        # id_ac为A群已种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
        id_ac = self.jzjl.query("id_x in @id_ac & vacc_months<=24 & vaccination_code in ['1702','1703','1704','5301']")['id_x']

        # A群已种
        id_2 = pd.concat([id_a, id_ac], ignore_index=True)

        # id_ac2 为AC群已种
        id_ac2 = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']
        id_acs = self.jzjl.query("id_x in @id_ac2 & age>=2")['id_x']
        id_3 = self.jzjl.query('id_x in @id_acs & id_x in @id_2')['id_x']

        menac1_subset2 = self.jzjl.query("id_x in @id_3 & age >= 2 & age<=6 & vaccine_name in ['流脑疫苗A群','流脑疫苗AC群'] ").assign(
            vaccination_date_plus_interval=lambda df: df['vaccination_date'] + pd.DateOffset(months=3),
            vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 13 * 2)
        ).loc[lambda df: ((df['vaccination_date'] <= df['vaccination_date_plus_interval']) & (df['vaccination_date'] > self.mon_end))]

        # 3. ≥2岁且已接种AC群流脑多糖疫苗第1剂，但未在应种日期范围内接种的儿童数。
        # id_ac为AC群已种
        id_ac = self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x']

        id_a = self.jzjc.query('vaccine_name == "流脑疫苗A群" & jzjc == 0')['id_x']

        id_3 = self.jzjl.query('id_x in @id_ac & id_x in @id_a')['id_x']
        menac1_subset3 = self.jzjl.query("id_x in @id_3 & age >= 2 & age<=6").assign(
            vaccination_date_plus_interval=lambda df: df['birth_date'] + pd.DateOffset(months=12 * 2),
            vaccination_date_plus_13_months=lambda df: df['birth_date'] + pd.DateOffset(months=13 + 12 * 2)
        ).loc[lambda df: ((df['vaccination_date'] > df['vaccination_date_plus_13_months']) & (df['vaccination_date'] > self.mon_end))]

        invalid = (
            pd.concat([menac1_subset1, menac1_subset2, menac1_subset3], ignore_index=True)
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '流脑疫苗AC群1')
        return result

    def MenAC2(self,  mon_stat):
        # 流脑AC2实种
        actual=(
            self.jzjl
            .query("(vaccine_name=='流脑疫苗AC群' & year_month==@mon_stat & jc==2 & age>=5 & age<18)")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 流脑AC2应种
        # 筛选出流脑AC疫苗且 jzjc 为 1 的记录
        id_1=self.jzjc.query('vaccine_name == "流脑疫苗AC群" & jzjc == 1')['id_x'].unique()
        expected = (
            self.jzjl
            .query("id_x in @id_1 & vacc_months >= 3*12 & vaccine_name == '流脑疫苗AC群' & jc == 1 ")
            .query("year_month < @mon_stat")
            .pipe(add_intervals, 'vaccination_date', 12*3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 流脑AC2已种，但不符合要求
        id_2=self.jzjl.query("vaccine_name == '流脑疫苗AC群' & jc == 2 & year_month > @mon_stat & vacc_months >= 12*3")['id_x'].unique()
        invalid=(
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '流脑疫苗AC群' & jc == 1")
            .pipe(add_intervals, 'vaccination_date', 12*3)
            .pipe(add_13_months, 'add_intervals')
            .loc[lambda df: (df['plus_13_months'] > self.mon_end) & (df['add_intervals'] <= self.mon_end)]
            .groupby(['current_management_code'])
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '流脑疫苗AC群2')
        return result

    def TD(self,  mon_stat):
        # 白破1实种
        actual = (
            self.jzjl
            .query("age<12 & vaccine_name=='白破疫苗' & year_month==@mon_stat & jc==1")
            .groupby(['vaccination_org'])
            .agg(actual_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 白破1应种
        # 筛选出白破1疫苗且 jzjc 为 0 的记录
        id_1 = self.jzjc.query('vaccine_name == "白破疫苗" & jzjc == 0')['id_x'].unique()
        expected = (
            self._df
            .query("id_x in @id_1 & age <= 11 & age>=6")
            .pipe(add_13_months, 'birth_date',6*12)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
            .groupby(['current_management_code'])
            .agg(expected_count=('id_x', 'nunique'))
            .reset_index()
        )

        # 不符合要求的1剂次接种
        id_2 = self.jzjl.query("vaccine_name == '白破疫苗' & jc == 1 & year_month > @mon_stat & age <= 11 & age>=6")['id_x'].unique()

        #和百白破疫苗间隔不足
        jiange= (
            self.jzjl[self.jzjl['vaccine_name'].isin(['百白破疫苗', '白破疫苗'])]
            .groupby('id_x')
            .filter(lambda x: (
                x[(x['vaccine_name'] == '百白破疫苗') & (x['jc'] >= 3)].shape[0] > 0 and
                x[(x['vaccine_name'] == '白破疫苗') & (x['jc'] == 1)].shape[0] > 0
            ))
            .groupby('id_x')
            .apply(lambda group: (
                group.assign(
                    max_bbp_date=group.loc[group['vaccine_name']
                                        == '百白破疫苗', 'vaccination_date'].max(),
                    jc_value=group.loc[group['vaccine_name'] == '百白破疫苗', 'jc'].max()
                )
                .query("vaccine_name == '白破疫苗'")
                .assign(
                    interval_months=lambda x: (
                        x['vaccination_date'] - x['max_bbp_date']) / timedelta(days=30)
                )
                .query("(jc_value == 4 and interval_months < 12) or (jc_value == 3 and interval_months < 6)")
            ))
            .reset_index(drop=True)
        )

        invalid = (
            self.jzjl
            .query("id_x in @id_2 & vaccine_name == '白破疫苗' & jc == 1")
            .pipe(add_13_months, 'birth_date',6*12)
            .loc[lambda df: df['plus_13_months'] > self.mon_end]
        )

        #合并间隔不足的
        invalid=(
            pd.concat([jiange[['id_x', 'current_management_code']], invalid[['id_x', 'current_management_code']]])
            .groupby('current_management_code')
            .agg(invalid_count=('id_x', 'nunique'))
            .reset_index()
        )

        result = calculate_vaccine_proportion(expected, actual, invalid, '白破疫苗1')
        return result

    def _update_vaccination_org(self):
        condition = (self.jzjl['vaccination_org'].isin(['777777777777', '888888888888', '999999999999'])) & \
                    (self.jzjl['vaccine_name'].isin(['乙肝疫苗', '卡介苗'])) & \
                    (self.jzjl['jc'] == 1)

        self.jzjl.loc[condition, 'vaccination_org'] = self.jzjl.loc[condition, 'entry_org']

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
            self.heva(),
            self.hbvj()
        ]
        return pd.concat(vaccines, ignore_index=True)

    def duration_rate(self, mon_stat):
        self.set_mon_end(mon_stat)
        vaccines = [
            self.BCG(mon_stat),
            self.HBV1(mon_stat),
            self.HBV2(mon_stat),
            self.HBV3(mon_stat),
            self.PV1(mon_stat),
            self.PV2(mon_stat),
            self.PV3(mon_stat),
            self.PV4(mon_stat),
            self.DPT1(mon_stat),
            self.DPT2(mon_stat),
            self.DPT3(mon_stat),
            self.DPT4(mon_stat),
            self.MCV1(mon_stat),
            self.MCV2(mon_stat),
            self.HepA(mon_stat),
            self.JEV1(mon_stat),
            self.JEV2(mon_stat),
            self.MenA1(mon_stat),
            self.MenA2(mon_stat),
            self.MenAC1(mon_stat),
            self.MenAC2(mon_stat)
        ]
        return pd.concat(vaccines, ignore_index=True)
    
    def cohort_rate(self, by_age=False):
        vaccine_df = self.all_vaccines()

        required_columns = ['current_management_code', 'age', 'vaccine_name', 'jc', 'id_x']
        missing_columns = [col for col in required_columns if col not in vaccine_df.columns]
        if missing_columns:
            raise ValueError(f"'vaccine_df' DataFrame 缺少以下列: {missing_columns}")

        if by_age:
            yingzhong = (
                self._df
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

            result = shizhong.merge(yingzhong, on=['current_management_code', 'age'], how='inner').reset_index(drop=True).assign(
                percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
            ).reset_index()

        else:
            yingzhong = (
                self._df
                .query("age >= 1")
                .groupby(['current_management_code'])
                .agg(cnt=('id_x', 'nunique'))
                .reset_index()
            )

            shizhong = (
                vaccine_df
                .query("age >= 1")
                .groupby(['current_management_code', 'vaccine_name', 'jc'])
                .agg(vac=('id_x', 'nunique'))
                .reset_index()
            )

            result = shizhong.merge(yingzhong, on=['current_management_code'], how='inner').reset_index(drop=True).assign(
                percent=lambda x: round(x['vac'] / x['cnt'] * 100, 2)
            ).reset_index()

        return result

