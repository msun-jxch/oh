import pandas as pd
import datetime
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.DEBUG)

tmp_path = r'../res/radiation_hazard_vs_oh_order_df.xlsx'
start_id = 5000
end_id = 6000
df_bjxm = pd.read_excel(r'../res/tmp_radiation_vs_oh_order_bjxm_df.xlsx', sheet_name='radiation_vs_oh_order_bjxm')
df_xjxm_chu_yingji = pd.read_excel(r'../res/tmp_radiation_vs_oh_order_xjxm_chu_yingji_df.xlsx',
                                   sheet_name='radiation_vs_xjxm_chu_yingji')

table_df = pd.concat([df_bjxm, df_bjxm, df_xjxm_chu_yingji])
table_df.reset_index(drop=True, inplace=True)
table_df.index = table_df.index + start_id
table_df['rel_oh_order_id'] = table_df.index
table_df.set_index(['rel_oh_order_id'], inplace=True)

logging.info("写入格式化数据")
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    table_df.to_excel(writer, 'hazard_vs_oh_order', index=True, header=True)

logging.info('导入数据')
table_df.drop(columns=['oh_order_id'], inplace=True)

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute(
        f"delete from hazard_vs_oh_order where rel_oh_order_id >= {start_id} and rel_oh_order_id <= {end_id}")
    table_df.to_sql('hazard_vs_oh_order', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)


logging.info('SUCCESS')
