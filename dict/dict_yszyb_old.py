import numpy as np
import pdfplumber
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, VARCHAR
from xpinyin import Pinyin

p = Pinyin()

path = r"..\res\职业病及健康危害因素监测信息系统数据交换文档规范.pdf"
now = datetime.now()
table_df = {}
table_columns = ['yszyb_code_upload', 'yszyb_name', 'remark']

with pdfplumber.open(path) as pdf:
    for i in range(176, 181):
        print('\r', f"{i} / 181", end='', flush=True)
        page = pdf.pages[i]
        if i == 176:
            table = page.extract_tables()[1]
            df = pd.DataFrame(table[1:])
            df = df.replace(to_replace='None', value=np.nan).dropna(axis=1)
            df.columns = table_columns
            table_df = df
        else:
            table = page.extract_tables()[0]
            df = pd.DataFrame(table)
            df = df.replace(to_replace='None', value=np.nan).dropna(axis=1)
            df.columns = table_columns
            table_df = pd.concat([table_df, df])

type_df = table_df.query('yszyb_code_upload.str.len()<3')
type_df.reset_index(drop=True, inplace=True)
table_df = table_df.query('yszyb_code_upload.str.len()>3')

table_df.reset_index(drop=True, inplace=True)
table_df['yszyb_id'] = table_df.index
table_df.set_index(['yszyb_id'], inplace=True)

table_df.drop(columns=['remark'], inplace=True)


full_code = pd.DataFrame(
    list(table_df['yszyb_name'].map(
        lambda x: ''.join([s.upper() for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['full_code'])
input_code = pd.DataFrame(
    list(table_df['yszyb_name'].map(
        lambda x: ''.join([s[0].upper() if len(s) > 0 else '' for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['input_code'])

code = pd.concat([full_code['full_code'], input_code['input_code']], axis=1)

table_df.reset_index(drop=True, inplace=True)
code.reset_index(drop=True, inplace=True)

table_df = pd.concat([table_df, code], axis=1)

table_df['yszyb_type_id'] = ''
table_df['yszyb_type_name'] = ''
table_df['yszyb_id'] = table_df.index
table_df.set_index(['yszyb_id'], inplace=True)

for index, row in table_df.iterrows():
    has = False
    for type_index2, type_row2 in type_df.iterrows():
        if row['yszyb_code_upload'][0:2] == type_row2['yszyb_code_upload']:
            row['yszyb_type_id'] = type_index2
            row['yszyb_type_name'] = type_row2['yszyb_name']
            has = True
            break
    if not has:
        for type_index, type_row in type_df.iterrows():
            if row['yszyb_code_upload'][0:1] == type_row['yszyb_code_upload']:
                row['yszyb_type_id'] = type_index
                row['yszyb_type_name'] = type_row['yszyb_name']
                break

table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '体检主任'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '体检主任'
table_df['his_update_time'] = now
table_df['delete_flag'] = 0

print("\n导入数据库...\n")

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@10.67.76.33:5432/chis')
    engine.execute("delete from dict_yszyb")
    table_df.to_sql('dict_yszyb', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(table_df)
