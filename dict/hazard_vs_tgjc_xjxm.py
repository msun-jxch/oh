import pandas as pd
import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin
from pandas import DataFrame
import re
import db_util.dict_post_status_util as dict_post_status_util
import db_util.dict_hazard_factor_type_util as dict_hazard_factor_type_util
import db_util.dict_oh_order_extend_util as dict_oh_order_extend_util
import logging

logging.basicConfig(level=logging.DEBUG)
p = Pinyin()
now = datetime.datetime.now()
tmp_path = r'../res/temp_hazard_vs_tgjc_xjxm_df.xlsx'


def format_str(the_df):
    the_df['危害因素'] = the_df['危害因素'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))
    the_df['选检项目'] = the_df['选检项目'].apply(str)
    the_df['选检项目'] = the_df['选检项目'].map(
        lambda name: name.replace('(', '（').replace(')', '）').replace(' ', '').split('（见附录')[0].strip(',').strip(
            ';'))
    return the_df


logging.info('读取初始数据')
table_df = DataFrame([])
df = pd.read_excel(r'../res/体检项目关联.xlsx', sheet_name='体格检查-选检')
df = format_str(df)
empty = ''
df = df.query('选检项目!=@empty').copy()

logging.info('写入清洗数据')
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    df.to_excel(writer, '选检项目', index=False, header=True)

logging.info('数据规整')
table_df['title_code'] = df['title_code']
table_df['order_id'] = df['选检项目'].map(lambda name: dict_oh_order_extend_util.get_order_id(name))
table_df['oh_order_id'] = df['选检项目'].map(lambda name: dict_oh_order_extend_util.get_oh_order_id(name))
table_df['oh_order_name'] = df['选检项目'].map(lambda name: dict_oh_order_extend_util.get_oh_order_name(name))
table_df['post_status_id'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_id(name))
table_df['post_status_name'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_name(name))
table_df['hazard_factor_id'] = df['危害因素'].map(lambda name: dict_hazard_factor_type_util.get_hazard_factor_id(name))

table_df.reset_index(drop=True, inplace=True)
table_df['rel_oh_order_id'] = table_df.index
table_df.set_index(['rel_oh_order_id'], inplace=True)

table_df['is_necessary'] = 1
table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '众阳健康管理员'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '众阳健康管理员'
table_df['his_update_time'] = now
table_df.drop(columns=['title_code'], inplace=True)

logging.info('写入格式化数据')
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='a') as writer:
    table_df.to_excel(writer, 'hazard_vs_tgjc_xjxm', index=False, header=True)

print(table_df)
