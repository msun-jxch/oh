import pandas as pd
import datetime
from sqlalchemy import create_engine
from xpinyin import Pinyin

p = Pinyin()
now = datetime.datetime.now()

yszzb_type = [
    '职业性尘肺病及其他呼吸系统疾病',
    '尘肺病',
    '其他呼吸系统疾病',
    '职业性皮肤病',
    '职业性眼病',
    '职业性耳鼻喉口腔疾病',
    '职业性化学中毒',
    '物理因素所致职业病',
    '职业性放射性疾病',
    '职业性传染病',
    '职业性肿瘤',
    '其他职业病',
]


def yszzb_type_id(contxt):
    for index, val in enumerate(yszzb_type):
        if val == contxt:
            return index
    return -1


df = pd.read_excel(r'../res/职业病.xlsx', sheet_name='GBZ188-2014')
df.rename(columns={'名称': 'yszyb_name'}, inplace=True)
df.rename(columns={'GBZ': 'diagnosis_criteria'}, inplace=True)
df.rename(columns={'职业病分类': 'yszyb_type_name'}, inplace=True)

df.drop(columns=['名称（GBZ）'], inplace=True)

df['yszyb_type_id'] = df['yszyb_type_name'].map(lambda x: yszzb_type_id(x))

full_code = pd.DataFrame(
    list(df['yszyb_name'].map(
        lambda x: ''.join([s.upper() for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['full_code'])
input_code = pd.DataFrame(
    list(df['yszyb_name'].map(
        lambda x: ''.join([s[0].upper() if len(s) > 0 else '' for s in
                           p.get_pinyin(x).replace('（', '').replace('）', '').replace('(', '').replace(')', '').replace(
                               '、', '').split(
                               '-')]))),
    columns=['input_code'])

code = pd.concat([full_code['full_code'], input_code['input_code']], axis=1)

df.reset_index(drop=True, inplace=True)
code.reset_index(drop=True, inplace=True)

df = pd.concat([df, code], axis=1)

df['hospital_id'] = 10033001
df['his_org_id'] = 10033
df['his_creater_id'] = 4796260644137206666
df['his_creater_name'] = '众阳健康管理员'
df['his_create_time'] = now
df['his_updater_id'] = 4796260644137206666
df['his_updater_name'] = '众阳健康管理员'
df['his_update_time'] = now
df['delete_flag'] = 0

df.reset_index(drop=True, inplace=True)
df['yszyb_id'] = df.index
df.set_index(['yszyb_id'], inplace=True)

print("\n导入数据库...\n")

try:
    engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
    engine.execute("delete from dict_yszyb")
    df.to_sql('dict_yszyb', engine, schema='oh', index=True, if_exists='append')
except Exception as e:
    print(e)

print(df)
