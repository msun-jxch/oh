import pandas as pd
import datetime
from sqlalchemy import create_engine
from pandas import DataFrame

from xpinyin import Pinyin

p = Pinyin()
engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()



# res = connection.execute(
#     """
#     SELECT *
#     FROM dict_oh_order_extend
#     WHERE is_custom_data=0
#     """
# )
#
# df = DataFrame(res.fetchall())
# df.columns = res.keys()
df = pd.read_excel(r'../res/temp_dict_order_extend_df.xlsx', sheet_name='dict_order_extend')
df.drop(columns=['full_code'], inplace=True)
df.drop(columns=['input_code'], inplace=True)

full_code = pd.DataFrame(
    list(df['oh_order_name'].map(
        lambda x: ''.join([s.upper() for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['full_code'])
input_code = pd.DataFrame(
    list(df['oh_order_name'].map(
        lambda x: ''.join([s[0].upper() if len(s) > 0 else '' for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['input_code'])

code = pd.concat([full_code['full_code'], input_code['input_code']], axis=1)

df.reset_index(drop=True, inplace=True)
code.reset_index(drop=True, inplace=True)

df = pd.concat([df, code], axis=1)

df['dict_order_extend_id'] = df['oh_order_id']
df['order_name'] = ''

connection.execute(
    "delete from dict_oh_order_extend where dict_order_extend_id in ("
    + ",".join(df['dict_order_extend_id'].astype(str).tolist()) + ")"
)
df.reset_index(drop=True, inplace=True)
df.set_index(['dict_order_extend_id'], inplace=True)
df.to_sql('dict_oh_order_extend', engine, schema='oh', index=True, if_exists='append')

with pd.ExcelWriter(r'../res/temp_dict_order_extend_df.xlsx', engine='openpyxl', mode='w') as writer:
    df.to_excel(writer, 'dict_order_extend', index=True, header=True)

print(df)
