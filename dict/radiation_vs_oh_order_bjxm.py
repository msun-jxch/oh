import pandas as pd
import logging
import datetime
from pandas import DataFrame
import db_util.dict_post_status_util as dict_post_status_util
import db_util.dict_hazard_factor_type_util as dict_hazard_factor_type_util
import db_util.dict_oh_order_extend_util as dict_oh_order_extend_util

logging.basicConfig(level=logging.DEBUG)

tmp_path = r'../res/tmp_radiation_vs_oh_order_bjxm_df.xlsx'
now = datetime.datetime.now()
logging.info("读取初始数据")
df_bjxm = pd.read_excel(r'../res/放射工作人员检查项目.xlsx', sheet_name='必检项目')

logging.info("数据清洗")
df_bjxm = pd.melt(df_bjxm, var_name='岗位状态', value_name='必检项目')
df_bjxm.dropna(axis=0, how='any', inplace=True)

df_bjxm['必检项目'] = df_bjxm['必检项目'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))

logging.info("写入清洗数据")
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    df_bjxm.to_excel(writer, '必检项目', index=False, header=True)

logging.info("数据规整")
table_df = DataFrame([])
table_df['order_id'] = df_bjxm['必检项目'].map(lambda name: dict_oh_order_extend_util.get_order_id(name))
table_df['oh_order_id'] = df_bjxm['必检项目'].map(lambda name: dict_oh_order_extend_util.get_oh_order_id(name))
table_df['oh_order_name'] = df_bjxm['必检项目'].map(lambda name: dict_oh_order_extend_util.get_oh_order_name(name))
table_df['post_status_id'] = df_bjxm['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_id(name))
table_df['post_status_name'] = df_bjxm['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_name(name))
table_df['hazard_factor_id'] = dict_hazard_factor_type_util.radiation_id()

table_df.reset_index(drop=True, inplace=True)
table_df['rel_oh_order_id'] = table_df.index
table_df.set_index(['rel_oh_order_id'], inplace=True)

table_df['is_necessary'] = 0
table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '众阳健康管理员'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '众阳健康管理员'
table_df['his_update_time'] = now
table_df['delete_flag'] = 0

table_df.reset_index(drop=True, inplace=True)
table_df['rel_oh_order_id'] = table_df.index
table_df.set_index(['rel_oh_order_id'], inplace=True)

logging.info("写入格式化数据")
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='a') as writer:
    table_df.to_excel(writer, 'radiation_vs_oh_order_bjxm', index=True, header=True)

logging.info('SUCCESS')
