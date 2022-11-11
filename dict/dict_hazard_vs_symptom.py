import pandas as pd
import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin
from pandas import DataFrame
import re
import db_util.dict_post_status_util as dict_post_status_util
import db_util.dict_hazard_factor_type_util as dict_hazard_factor_type_util
import db_util.dict_oh_order_extend_util as dict_oh_order_extend_util
import db_util.dict_symptom_util as dict_symptom_util
import logging

logging.basicConfig(level=logging.DEBUG)
p = Pinyin()
now = datetime.datetime.now()
tmp_path = r'../res/tmp_dict_hazard_vs_symptom_df.xlsx'


def tong(tong_df):
    for index, row in tong_df.iterrows():
        if re.search('^同[0-9.]*', row['症状询问']) is not None:
            title_code = row['症状询问'].strip('同').strip('a)').strip('a）').strip(';').strip('；').strip('。').strip('.')
            tong_cp = tong_df.query('title_code==@title_code').copy()
            tong_cp['title_code'] = row['title_code']
            tong_cp['危害因素'] = row['危害因素']
            tong_cp['岗位状态'] = row['岗位状态']
            tong_df.drop(tong_df[(tong_df['症状询问'] == row['症状询问'])].index, inplace=True)
            tong_df = tong_df.append(tong_cp, ignore_index=True)
    return tong_df


def format_str(the_df):
    the_df['危害因素'] = the_df['危害因素'].map(lambda name: name.replace('(', '（').replace(')', '）').replace(' ', ''))
    the_df['症状询问'] = the_df['症状询问'].map(
        lambda name: name.replace('(', '（').replace(')', '）').replace(' ', '').split('（见附录')[0].strip(',').strip(
            ';'))
    return the_df


logging.info('读取初始数据')
table_df_tmp = DataFrame([])
df = pd.read_excel(r'../res/症状问讯关联.xlsx', sheet_name='GBZ188-2014')
df = tong(df)
df = format_str(df)

logging.info('写入清洗数据')
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='w') as writer:
    df.to_excel(writer, '症状询问', index=False, header=True)

logging.info('数据规整')
table_df_tmp['title_code'] = df['title_code']
table_df_tmp['症状询问'] = df['症状询问']
table_df_tmp['post_status_id'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_id(name))
table_df_tmp['post_status_name'] = df['岗位状态'].map(lambda name: dict_post_status_util.get_post_status_name(name))
table_df_tmp['hazard_factor_id'] = df['危害因素'].map(
    lambda name: dict_hazard_factor_type_util.get_hazard_factor_id(name))

table_df_tmp['hospital_id'] = 10033001
table_df_tmp['his_org_id'] = 10033
table_df_tmp['his_creater_id'] = 4796260644137206666
table_df_tmp['his_creater_name'] = '众阳健康管理员'
table_df_tmp['his_create_time'] = now
table_df_tmp['his_updater_id'] = 4796260644137206666
table_df_tmp['his_updater_name'] = '众阳健康管理员'
table_df_tmp['his_update_time'] = now

logging.info('写入中间数据')
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='a') as writer:
    table_df_tmp.to_excel(writer, '中间数据', index=False, header=True)

logging.info('数据规整')
table_columns = ['hazard_factor_id', 'post_status_id', 'post_status_name', 'symptom_id', 'symptom_name',
                 'hospital_id', 'his_org_id', 'his_creater_id', 'his_creater_name', 'his_create_time',
                 'his_updater_id', 'his_updater_name', 'his_update_time']
table_df = DataFrame(columns=table_columns)
for index, row in table_df_tmp.iterrows():
    symptoms = dict_symptom_util.get_symptoms(row['症状询问'])
    for symptom in symptoms:
        new_row = row.copy()
        new_row['symptom_id'] = symptom['symptom_id']
        new_row['symptom_name'] = symptom['symptom_name']
        table_df = pd.concat([table_df, DataFrame([{
            'hazard_factor_id': new_row['hazard_factor_id'],
            'post_status_id': new_row['post_status_id'],
            'post_status_name': new_row['post_status_name'],
            'symptom_id': new_row['symptom_id'],
            'symptom_name': new_row['symptom_name'],
            'hospital_id': new_row['hospital_id'],
            'his_org_id': new_row['his_org_id'],
            'his_creater_id': new_row['his_creater_id'],
            'his_creater_name': new_row['his_creater_name'],
            'his_create_time': new_row['his_create_time'],
            'his_updater_id': new_row['his_updater_id'],
            'his_updater_name': new_row['his_updater_name'],
            'his_update_time': new_row['his_update_time'],
        }])])

table_df.reset_index(drop=True, inplace=True)
table_df['rel_symptom_id'] = table_df.index
table_df.set_index(['rel_symptom_id'], inplace=True)

logging.info('写入格式化数据')
with pd.ExcelWriter(tmp_path, engine='openpyxl', mode='a') as writer:
    table_df.to_excel(writer, 'dict_hazard_vs_symptom', index=True, header=True)

logging.info('写入数据库')
try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute("delete from dict_hazard_vs_symptom")
    table_df.to_sql('dict_hazard_vs_symptom', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

logging.info('SUCCESS')
