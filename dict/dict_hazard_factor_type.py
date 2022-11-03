import pandas as pd
import datetime
from sqlalchemy import create_engine

now = datetime.datetime.now()

hazard_type = {
    '5': {'hazard_type_name': '化学有害因素', 'hazard_type_id': 12},
    '6': {'hazard_type_name': '粉尘', 'hazard_type_id': 11},
    '7': {'hazard_type_name': '物理有害因素', 'hazard_type_id': 13},
    '8': {'hazard_type_name': '生物因素', 'hazard_type_id': 14},
    '9': {'hazard_type_name': '特殊作业', 'hazard_type_id': 15},
}

df = pd.read_excel(r'../res/危害因素关联.xlsx', sheet_name='危害因素列表')

df.rename(columns={'危害因素': 'hazard_factor_name'}, inplace=True)


df.reset_index(drop=True, inplace=True)
df['hazard_factor_id'] = df.index
df.set_index(['hazard_factor_id'], inplace=True)
df.loc[df['hazard_factor_name'].str.contains('（CASNo：'), 'hazard_factor_cas'] = \
df['hazard_factor_name'].str.split('CASNo：', expand=True)[1].str.split('）', expand=True)[0]
df['hazard_factor_name'] = df['hazard_factor_name'].str.split('（CASNo：', expand=True)[0]

df['title_code'] = df['title_code'].apply(str)
df.loc[df['title_code'].str.contains('5.') ,'hazard_type_id'] = '12'
df.loc[df['title_code'].str.contains('6.') ,'hazard_type_id'] = '11'
df.loc[df['title_code'].str.contains('7.') ,'hazard_type_id'] = '13'
df.loc[df['title_code'].str.contains('8.') ,'hazard_type_id'] = '14'
df.loc[df['title_code'].str.contains('9.') ,'hazard_type_id'] = '15'

df.loc[df['title_code'].str.contains('5.') ,'hazard_type_name'] = '化学有害因素'
df.loc[df['title_code'].str.contains('6.') ,'hazard_type_name'] = '粉尘'
df.loc[df['title_code'].str.contains('7.') ,'hazard_type_name'] = '物理有害因素'
df.loc[df['title_code'].str.contains('8.') ,'hazard_type_name'] = '生物因素'
df.loc[df['title_code'].str.contains('9.') ,'hazard_type_name'] = '特殊作业'

df.drop(columns=['title_code'], inplace=True)

df['hospital_id'] = 10033001
df['his_org_id'] = 10033

df['his_creater_id'] = 4796260644137206666
df['his_creater_name'] = '体检主任'
df['his_create_time'] = now
df['his_updater_id'] = 4796260644137206666
df['his_updater_name'] = '体检主任'
df['his_update_time'] = now

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@10.67.76.33:5432/chis')
    engine.execute("delete from dict_hazard_factor_type")
    df.to_sql('dict_hazard_factor_type', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(df)
