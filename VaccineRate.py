import pandas as pd
import numpy as np
import janitor

class VaccineRate(pd.DataFrame):
    _metadata = ['tj_date', 'vacc_code', 'vacc_seq', 'id', 'person']

    def __init__(self, person, id, tj_date, vacc_code, vacc_seq, *args, **kwargs):
        super().__init__(person, *args, **kwargs)
        self.person = person
        self.tj_date = tj_date
        self.vacc_code = vacc_code
        self.vacc_seq = vacc_seq
        self.id = id


    def clean(self):
        self['birth_date'] = pd.to_datetime(self['birth_date'].astype(str).str.split('T').str[0], format='%Y%m%d')
        self['age'] = np.floor((pd.to_datetime(self.tj_date, format='%Y-%m-%d') - self['birth_date']).dt.days / 365.25).astype(int)
        self.query("age >= 1 & age <= 18", inplace=True)
    

    def calculate(self):
        # 预先计算所需的列，避免在groupby中重复调用
        self['vaccinated'] = (
            (self['vaccination_code'] == self.vacc_code) 
            # & (self['vaccination_seq'] == self.vacc_seq)
        )
        
        result = self.groupby(['age','vaccination_seq']).agg(
            cnt=(self.id, 'nunique'),
            vac=(self.id, lambda x: x[self.loc[x.index, 'vaccinated']].nunique())
        ).reset_index()

        return result

# person2=person.merge(person__vaccination,how='left',left_on='id',right_on='person_id')

# tmp = VaccineRate(person.query("current_management_code == '333647265032'"), 'id_x', '2021-12-31', '0101', 1)
tmp = VaccineRate(person,'id_x', '2021-12-31', '0101', 1)
tmp.query("current_management_code == '333647265032'", inplace=True)
tmp.clean()
result = tmp.calculate()

class VaccineRate2(pd.DataFrame):
    def __init__(self, person, tj_date, *args, **kwargs):
        super().__init__(person, *args, **kwargs)
        self.person = person
        self.tj_date = tj_date
        
        self['birth_date'] = pd.to_datetime(self['birth_date'].astype(str).str.split('T').str[0], format='%Y%m%d')
        self['age'] = np.floor((pd.to_datetime(self.tj_date, format='%Y-%m-%d') - self['birth_date']).dt.days / 365.25).astype(int)
        self.query("age >= 1 & age <= 18", inplace=True)


class HBV(VaccineRate2):
    
    def __init__(self, person, tj_date,id, *args, **kwargs):
        super().__init__(person, tj_date)
        self.id = id
        self.vacc_code = ['0201', '0202', '0203']

    def arrange(self):
        self=self.query("vaccination_code.isin(@self.vacc_code)")  
        self['grp'] = 'HBV'
        return self
    
HBV = HBV(person2,'2021-12-31','id_x').arrange()







