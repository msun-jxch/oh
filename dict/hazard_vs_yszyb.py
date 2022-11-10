import db_util.dict_yszyb_util as dict_yszyb_util
import db_util.dict_post_status_util as dict_post_status_util
import db_util.dict_hazard_factor_type_util as dict_hazard_factor_type_util
import pandas as pd
import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin
from pandas import DataFrame
import re
import logging

logging.basicConfig(level=logging.DEBUG)


def tong(tong_df):
    for index, row in df.iterrows():
        if re.search('^同[0-9.]*', row['职业病']) is not None:
            title_code = row['职业病'].strip('同')
            tong_cp = tong_df.query('title_code==@title_code').copy()
            tong_cp['title_code'] = row['title_code']
            tong_cp['危害因素'] = row['危害因素']
            tong_cp['危害因素'] = row['危害因素']
            tong_cp['岗位状态'] = row['岗位状态']
            tong_df.drop(tong_df[(tong_df['职业病'] == row['职业病'])].index, inplace=True)
            tong_df = tong_df.append(tong_cp, ignore_index=True)
    return tong_df


def format_str(the_df):
    the_df['危害因素'] = the_df['危害因素'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))
    the_df['职业病'] = the_df['职业病'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))
    return the_df


now = datetime.datetime.now()
tmp_path = r'../res/temp_hazard_vs_yszzb_df.xlsx'
table_df = DataFrame([])
df = pd.read_excel(r'../res/职业病关联.xlsx', sheet_name='GBZ188-2014')
df = tong(df)
df = format_str(df)

with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    df.to_excel(writer, '数据清洗', index=False, header=True)

table_df['title_code'] = df['title_code']
table_df['yszyb_id'] = df['职业病'].map(lambda name: dict_yszyb_util.get_yszyb_id(name))
table_df['yszyb_name'] = df['职业病'].map(lambda name: dict_yszyb_util.get_yszyb_name(name))
table_df['post_status_id'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_id(name))
table_df['post_status_name'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_name(name))
table_df['hazard_factor_id'] = df['危害因素'].map(lambda name: dict_hazard_factor_type_util.get_hazard_factor_id(name))

table_df.reset_index(drop=True, inplace=True)
table_df['hazard_vs_yszyb_id'] = table_df.index
table_df.set_index(['hazard_vs_yszyb_id'], inplace=True)

table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '众阳健康管理员'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '众阳健康管理员'
table_df['his_update_time'] = now
table_df.drop(columns=['title_code'], inplace=True)

with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='a') as writer:
    table_df.to_excel(writer, 'hazard_vs_yszyb', index=False, header=True)

logging.info('导入数据库...')

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute("delete from hazard_vs_yszyb")
    table_df.to_sql('hazard_vs_yszyb', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

logging.info('SUCCESS!')
