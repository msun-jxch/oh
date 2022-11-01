import numpy as np
import pdfplumber
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin

p = Pinyin()

path = r"..\res\职业病及健康危害因素监测信息系统数据交换文档规范.pdf"
now = datetime.now()
table_df = {}
table_columns = ['dict_item_id', 'item_name', 'remark']

with pdfplumber.open(path) as pdf:
    for i in range(51, 72):
        print('\r', f"{i} / 72", end='', flush=True)
        page = pdf.pages[i]
        if i == 51:
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

full_code = pd.DataFrame(
    list(table_df['item_name'].map(
        lambda x: ''.join([s.upper() for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['full_code'])
input_code = pd.DataFrame(
    list(table_df['item_name'].map(
        lambda x: ''.join([s[0].upper() if len(s) > 0 else '' for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['input_code'])

code = pd.concat([full_code['full_code'], input_code['input_code']], axis=1)

table_df.reset_index(drop=True, inplace=True)
code.reset_index(drop=True, inplace=True)

table_df = pd.concat([table_df, code], axis=1)

table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '体检主任'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '体检主任'
table_df['his_update_time'] = now
table_df['sex_id'] = 0
table_df['result_type'] = 0
table_df['delete_flag'] = 0
table_df['is_custom_data'] = 0
table_df['is_confirm'] = 0

table_df.set_index(['dict_item_id'], inplace=True)
table_df.drop(columns=['remark'], inplace=True)

print("\n导入数据库...\n")

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@10.67.76.33:5432/chis')
    engine.execute("delete from dict_oh_item")
    table_df.to_sql('dict_oh_item', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(table_df)
