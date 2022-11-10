from sqlalchemy import create_engine
from pandas import DataFrame

engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()

res = connection.execute(
    """
    SELECT *
    FROM dict_oh_order_extend
    WHERE is_custom_data=0
    """
)
df = DataFrame(res.fetchall())
df.columns = res.keys()


def get_order_id(oh_order_name):
    for index, row in df.iterrows():
        if oh_order_name == row['oh_order_name']:
            return row['order_id']
    return -1


def get_oh_order_id(oh_order_name):
    for index, row in df.iterrows():
        if oh_order_name == row['oh_order_name']:
            return row['oh_order_id']
    return -1


def get_dict_order_extend_id(oh_order_name):
    for index, row in df.iterrows():
        if oh_order_name == row['oh_order_name']:
            return row['dict_order_extend_id']
    return -1


def get_dict_order_name(oh_order_name):
    for index, row in df.iterrows():
        if oh_order_name == row['oh_order_name']:
            return row['order_name']
    return -1


def get_oh_order_name(oh_order_name):
    for index, row in df.iterrows():
        if oh_order_name == row['oh_order_name']:
            return row['oh_order_name']
    return oh_order_name
