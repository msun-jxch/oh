from sqlalchemy import create_engine
from pandas import DataFrame

engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()

res = connection.execute(
    """
    SELECT *
    FROM dict_post_status
    """
)
df = DataFrame(res.fetchall())
df.columns = res.keys()


def get_post_status_id(post_status_name):
    post_status_name = '在岗期间' if '在岗职业健康检查' == post_status_name else post_status_name
    for index, row in df.iterrows():
        row_name = row['post_status_name'].split('（')[0].split('[')[0].strip()
        if row_name in post_status_name:
            return row['post_status_id']
    return -1


def get_post_status_name(post_status_name):
    post_status_name = '在岗期间' if '在岗职业健康检查' == post_status_name else post_status_name
    for index, row in df.iterrows():
        row_name = row['post_status_name'].split('（')[0].split('[')[0].strip()
        if row_name in post_status_name:
            return row['post_status_name']
    return post_status_name
