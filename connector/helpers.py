import pyodbc, json
from myconfig import *

def get_json(file):
    with open(file) as basefile:
        base = json.loads(basefile.read())
    return base

def get_cnxn():
    cnxn = pyodbc.connect(f'DRIVER={drv};SERVER={srv};DATABASE={db};UID={usr};PWD={pw}')
    return cnxn

def get_data():
    cnxn = get_cnxn()
    cursor = cnxn.cursor()
    sql = """
    select jobdisplayid, c.fullname
    from job.jobs j 
    join job.jobcontactdetail cd on j.customerdetailid = cd.id
    join crm.contact c on cd.contactid = c.id
    where jobdisplayid in (1358607, 1358348)
    """
    cursor.execute(sql)
    return cursor.fetchall()

def apply_data_to_base(row):
    base = get_json('connector/base/post_order.json')
    base['orderNumber'] = row[0]

def load_rows(data):
    for row in data:
        payload = apply_data_to_base(row)
        #send payload to api