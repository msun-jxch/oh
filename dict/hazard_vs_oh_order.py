import pandas as pd
import datetime
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.DEBUG)

tmp_path = r'../res/hazard_vs_oh_order_df.xlsx'

df_bjxm = pd.read_excel(r'../res/temp_hazard_vs_bjxm_df.xlsx', sheet_name='hazard_vs_bjxm')
df_xjxm = pd.read_excel(r'../res/temp_hazard_vs_xjxm_df.xlsx', sheet_name='hazard_vs_xjxm')
df_tgjc_bjxm = pd.read_excel(r'../res/temp_hazard_vs_tgjc_bjxm_df.xlsx', sheet_name='hazard_vs_tgjc_bjxm')
df_tgjc_xjxm = pd.read_excel(r'../res/temp_hazard_vs_tgjc_xjxm_df.xlsx', sheet_name='hazard_vs_tgjc_xjxm')


table_df = pd.concat([df_bjxm, df_xjxm, df_tgjc_bjxm, df_tgjc_xjxm])
table_df.reset_index(drop=True, inplace=True)
table_df['rel_oh_order_id'] = table_df.index
table_df.set_index(['rel_oh_order_id'], inplace=True)

with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    table_df.to_excel(writer, 'hazard_vs_oh_order', index=True, header=True)

logging.info('导入数据')
# table_df.drop(columns=['oh_order_id'], inplace=True)
table_df['delete_flag'] = 0
try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute("delete from hazard_vs_oh_order")
    table_df.to_sql('hazard_vs_oh_order', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

logging.info('SUCCESS')

