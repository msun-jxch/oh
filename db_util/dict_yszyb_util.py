from sqlalchemy import create_engine
from pandas import DataFrame

engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()

res = connection.execute(
    """
    SELECT *
    FROM dict_yszyb
    """
)
df = DataFrame(res.fetchall())
df.columns = res.keys()


def get_yszyb_id(yszyb_name):
    name = yszyb_name.split('（')[0].split('[')[0].strip()
    for index, row in df.iterrows():
        row_name = row['yszyb_name'].split('（')[0].split('[')[0].strip()
        if name == row_name:
            return row['yszyb_id']
    return -1


def get_yszyb_name(yszyb_name):
    name = yszyb_name.split('（')[0].split('[')[0].strip()
    for index, row in df.iterrows():
        row_name = row['yszyb_name'].split('（')[0].split('[')[0].strip()
        if name == row_name:
            return row['yszyb_name']
    return yszyb_name
