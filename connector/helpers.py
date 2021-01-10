import json, pyodbc, requests
from myconfig import *

def sql_get_pickups():
    return """
        select top 2 jobdisplayid as orderNumber
            ,l.CompanyName as customerName
            ,la.Address1 + isnull(' '+la.Address2,'') + ', ' + la.City + ', ' + la.[State] + ' ' + la.ZipCode as customerAddress
            ,l.CPrimaryEmail as customerEmail
            ,l.CPrimaryPhone as customerPhoneNumber
            ,ISNULL(pcc.CompanyName + ' (' + pc.FullName + ')', pc.FullName ) as restaurantName
            ,pa.Address1 + isnull(' '+pa.Address2,'') + ', ' + pa.City + ', ' + pa.[State] + ' ' + pa.ZipCode as restaurantAddress
            ,pp.Phone as restaurantPhoneNumber
            ,pa.Latitude as pickupLatitude
            ,pa.Longitude as pickupLongitude
            ,convert(varchar(8), cast(put.TargetDate as time)) as expectedPickupTime
        from job.jobs j
            join company.Company l on j.CompanyID = l.CompanyID
                join [crm].[CompanyAddressMapping] lam on l.CompanyID = lam.CompanyId
                join crm.Address la on lam.AddressId = la.Id
            join job.JobContactDetail pd on j.PickupDetailId = pd.Id
                join crm.Contact pc on pd.ContactId = pc.Id
                left outer join company.Company pcc on pc.CompanyId = pcc.CompanyID
                left outer join crm.Address pa on pd.AddressId = pa.Id
                left outer join crm.Phone pp on pd.PhoneId = pp.Id
            join job.TimeLine tl on j.JobID = tl.JobID and tl.TaskCode = 'pu'
                join task.Task put on tl.TaskID = put.TaskID
        where l.CompanyCode = '1062tx' and put.TargetDate between convert(date, GETDATE()) and DATEADD(day,1,convert(date, GETDATE()))
        """

def get_json(file):
    with open(file) as basefile:
        base = json.loads(basefile.read())
    return base

def get_cnxn():
    cnxn = pyodbc.connect(f'DRIVER={drv};SERVER={srv};DATABASE={db};UID={usr};PWD={pw}')
    return cnxn

def get_data():
    url = "https://dispatch.shipday.com/orders"
    base = get_json('connector/base/post_order.json')
    cursor = get_cnxn().cursor().execute(sql_get_pickups())
    columns = [column[0] for column in cursor.description]
    output = []
    for row in cursor.fetchall():
        payload = dict(zip(columns, [str(v) for v in row]))
        response = requests.request("POST", url, headers=shipday_headers, data=json.dumps(payload))
        output.append(payload)
        print(response.text.encode('utf8'))
    return output