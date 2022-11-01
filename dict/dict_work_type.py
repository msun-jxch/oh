import numpy as np
import pdfplumber
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, VARCHAR

path = r"..\res\职业病及健康危害因素监测信息系统数据交换文档规范.pdf"
now = datetime.now()
table_df = {}
table_columns = ['work_type_code', 'work_type_name', 'remark']

with pdfplumber.open(path) as pdf:
    for i in range(82, 177):
        print('\r', f"{i} / 177", end='', flush=True)
        page = pdf.pages[i]
        if i == 82:
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
            df['work_type_id'] = df.index
            table_df = pd.concat([table_df, df])

table_df.reset_index(drop=True, inplace=True)
table_df['work_type_id'] = table_df.index
table_df.set_index(['work_type_code'], inplace=True)

table_df['hospital_id'] = 10033001
table_df['his_org_id'] = 10033
table_df['his_creater_id'] = 4796260644137206666
table_df['his_creater_name'] = '体检主任'
table_df['his_create_time'] = now
table_df['his_updater_id'] = 4796260644137206666
table_df['his_updater_name'] = '体检主任'
table_df['his_update_time'] = now

print("\n导入数据库...\n")

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@10.67.76.33:5432/chis')
    engine.execute("delete from dict_work_type")
    table_df.to_sql('dict_work_type', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(table_df)
