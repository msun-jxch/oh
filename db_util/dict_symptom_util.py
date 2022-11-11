from sqlalchemy import create_engine
from pandas import DataFrame

engine = create_engine('postgresql://oh_app:55EPU7v1aYHQ3DLvARPcZtSr@dev.db.oh.msun:5432/chis')
connection = engine.connect()

res = connection.execute('SELECT * FROM dict_symptom')
df = DataFrame(res.fetchall())
df.columns = res.keys()

qt_name = '其他'
qt = df.query('symptom_name==@qt_name').copy()
qt_id = qt['symptom_id'].tolist()[0]


def get_symptoms(context):
    symptoms = []
    for index, row in df.iterrows():
        row_name = row['symptom_name']
        if row_name in context:
            symptoms.append({'symptom_name': row_name, 'symptom_id': row['symptom_id']})

    symptoms.append({'symptom_name': context, 'symptom_id': qt_id})
    return symptoms
