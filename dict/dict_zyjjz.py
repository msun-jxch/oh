import pandas as pd
import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin

p = Pinyin()
now = datetime.datetime.now()

df = pd.read_excel(r'../res/职业禁忌症.xlsx', sheet_name='GBZ188-2014')
df.rename(columns={'名称': 'zyjjz_name'}, inplace=True)

df.reset_index(drop=True, inplace=True)
df['zyjjz_id'] = df.index
df.set_index(['zyjjz_id'], inplace=True)
df['zyjjz_name'] = df['zyjjz_name'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))

df['hospital_id'] = 10033001
df['his_org_id'] = 10033
df['his_creater_id'] = 4796260644137206666
df['his_creater_name'] = '众阳健康管理员'
df['his_create_time'] = now
df['his_updater_id'] = 4796260644137206666
df['his_updater_name'] = '众阳健康管理员'
df['his_update_time'] = now

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute("delete from dict_zyjjz")
    df.to_sql('dict_zyjjz', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(df)
