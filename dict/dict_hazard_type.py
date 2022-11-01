import pandas as pd
import datetime
from sqlalchemy import create_engine

now = datetime.datetime.now()

df = pd.read_excel(r'../res/字典编码标准库.xls', sheet_name='3.12危害因素编码')

df.rename(columns={'3.12危害因素编码': 'hazard_type_code_upload'}, inplace=True)
df.rename(columns={'Unnamed: 2': 'hazard_type_name'}, inplace=True)
df.drop(columns=['Unnamed: 0'], inplace=True)
df.drop(axis=0, index=0, inplace=True)

df = df.query('hazard_type_code_upload<100')

df.reset_index(drop=True, inplace=True)
df['hazard_type_id'] = df.index
df.set_index(['hazard_type_id'], inplace=True)

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
    df.to_sql('dict_hazard_type', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)


print(df)
