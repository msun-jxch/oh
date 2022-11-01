import pandas as pd
import datetime
from sqlalchemy import create_engine

now = datetime.datetime.now()

df = pd.read_excel(r'../res/字典编码标准库.xls', sheet_name='3.21工种代码')
df.rename(columns={'3.21工种代码': 'work_type_code'}, inplace=True)
df.rename(columns={'Unnamed: 2': 'work_type_name'}, inplace=True)
df.drop(columns=['Unnamed: 0'], inplace=True)
df.drop(axis=0, index=0, inplace=True)
df['work_type_id'] = df.index
df.set_index(['work_type_code'], inplace=True)
# df.reset_index(drop=True, inplace=True)
df['hospital_id'] = 10033001
df['his_org_id'] = 10033

df['his_creater_id'] = 4796260644137206666
df['his_creater_name'] = '体检主任'
df['his_create_time'] = now
df['his_updater_id'] = 4796260644137206666
df['his_updater_name'] = '体检主任'
df['his_update_time'] = now

try:
    engine = create_engine('postgresql://root:jiang155.@127.0.0.1:5432/chis')
    df.to_sql('dict_work_type', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(df)
