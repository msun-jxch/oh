from sqlalchemy import create_engine
from pandas import DataFrame

engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()

res = connection.execute(
    """
    SELECT *
    FROM dict_hazard_factor_type
    """
)
df = DataFrame(res.fetchall())
df.columns = res.keys()


def get_hazard_factor_id(hazard_factor_name):
    name = hazard_factor_name.split('（')[0].split('[')[0].strip()
    for index, row in df.iterrows():
        row_name = row['hazard_factor_name'].split('（')[0].split('[')[0].strip()
        if name == row_name:
            return row['hazard_factor_id']

    return -1


def radiation_id():
    return '100'
