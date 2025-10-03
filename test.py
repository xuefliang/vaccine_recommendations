import pandas as pd
from dateutil.relativedelta import relativedelta
yingzhong=(
    pd.read_excel("/mnt/d/标准库接种率/doc/标准库国家计算结果（默认版本）/机构数据334878619388/期间接种率/接种率明细数据/334878619388-2021-01-01 00_00_00-2021-01-31 23_59_59 期间接种率明细.xlsx",sheet_name='脊灰2')
    .query("index<=11")
)


tmp = yingzhong.loc[~yingzhong['个案ID'].isin(actual['id_x'])]



tmp=expected.loc[~expected['id_x'].isin(yingzhong['个案ID'])]

tmp2=actual.loc[~actual['id_x'].isin(yingzhong['个案ID'])]


###
last_day_of_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd.offsets.MonthEnd(0)

first_day_of_month = pd.Timestamp(current_date) - relativedelta(months=12, day=1)

recommendation['recommended_date'] = pd.to_datetime(recommendation['recommended_date'])
recommendation['vaccination_date'] = pd.to_datetime(recommendation['vaccination_date'])

actual = (
    recommendation
    .loc[
        (recommendation['vaccination_name'] == '脊灰疫苗_2') &
        (recommendation['vaccination_date'].dt.year == current_date.year) &
        (recommendation['vaccination_date'].dt.month == current_date.month) &
        (recommendation['age'] >= 0) &
        (recommendation['age'] < 18*12) &
        (recommendation['vaccination_date'].notnull()) &
        (recommendation['vaccination_org'] == recommendation['current_management_code'])
    ]
)

expected = (
   recommendation
    .loc[(recommendation.recommended_date >= first_day_of_month) &
        (recommendation.recommended_date <= last_day_of_month) &
        (recommendation.age >= 1) & 
        (recommendation.age < 6*12) &
        (recommendation.vaccination_name == '脊灰疫苗_1')
    ]
)

tmp2=person.filter(_.id_x=='618eeeaedae4474993071697b9e7fef1').to_pandas()



import ibis
from ibis import _
import ibis.selectors as s

starwars = ibis.examples.starwars.fetch()
starwars.head(6)
starwars.filter(_.skin_color == "light")
starwars.filter([_.skin_color == "light", _.eye_color == "brown"])
starwars.filter(
    ((_.skin_color == "light") & (_.eye_color == "brown")) |
    (_.species == "Droid")
)
starwars.order_by(_.height)
starwars.select(_.hair_color, _.skin_color, _.eye_color)

starwars.relabel(dict(homeworld='home_world'))

starwars.rename({'skincolor': 'skin_color'})

starwars.select(s.contains("world"))
starwars_colors = (
    starwars
        .select("name", s.matches("color"))
        .pivot_longer(s.matches("color"), names_to="attribute", values_to="color")
)

(
    starwars_colors.
        pivot_wider(names_from="attribute", values_from="color")
)

starwars.rename({'homeworld': 'home_world'})

from docx import Document
def count_images_tables(doc_path):
    doc = Document(doc_path)
    images_count = 0
    tables_count = len(doc.tables)
    for rel in doc.part.rels.values():
        if "image" in rel.target_ref:
            images_count += 1
    return images_count, tables_count

doc_path = '/mnt/d/书/甘肃省免疫规划信息系统应用指南.docx'
images, tables = count_images_tables(doc_path)
print(f"图片数量: {images}, 表格数量: {tables}")


person2=person.to_pandas()


