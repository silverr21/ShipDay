import json, pyodbc, requests
from myconfig import *

def sql_get_pickups():
    return """
        select jobdisplayid as orderNumber
            ,l.CompanyName as customerName
            ,la.Address1 + isnull(' '+la.Address2,'') + ', ' + la.City + ', ' + la.[State] + ' ' + la.ZipCode as customerAddress
            ,l.CPrimaryEmail as customerEmail
            ,l.CPrimaryPhone as customerPhoneNumber
            ,ISNULL(pcc.CompanyName + ' (' + pc.FullName + ')', pc.FullName ) as restaurantName
            ,pa.Address1 + isnull(' '+pa.Address2,'') + ', ' + pa.City + ', ' + pa.[State] + ' ' + pa.ZipCode as restaurantAddress
            ,pp.Phone as restaurantPhoneNumber
            ,pa.Latitude as pickupLatitude
            ,pa.Longitude as pickupLongitude
            ,convert(varchar(8), cast(put.TargetDate as time)) as expectedDeliveryTime
        from job.jobs j
            join company.Company l on j.CompanyID = l.CompanyID
                join [crm].[CompanyAddressMapping] lam on l.CompanyID = lam.CompanyId
                join crm.Address la on lam.AddressId = la.Id
            join job.JobContactDetail pd on j.PickupDetailId = pd.Id
                join crm.Contact pc on pd.ContactId = pc.Id
                left outer join company.Company pcc on pc.CompanyId = pcc.CompanyID
                left outer join crm.Address pa on pd.AddressId = pa.Id
                left outer join crm.Phone pp on pd.PhoneId = pp.Id
            left outer join job.TimeLine tl on j.JobID = tl.JobID and tl.TaskCode = 'pu'
                left outer join task.Task put on tl.TaskID = put.TaskID
            left outer join job.ShipmentPlan sp on j.JobID = sp.JobID and sp.SequenceNo = 6 and sp.PickUp = l.CompanyID
        where l.CompanyCode = '1062tx' and put.TargetDate between DATEADD(day,0,convert(date, GETDATE())) and DATEADD(day,1,convert(date, GETDATE())) 
            and (j.DelSDoneBy <> 'A' or sp.PickUp <> l.CompanyID)
        union
        select jobdisplayid as orderNumber
            ,ISNULL(dcc.CompanyName + ' (' + dc.FullName + ')', dc.FullName ) as customerName
            ,da.Address1 + isnull(' '+da.Address2,'') + ', ' + da.City + ', ' + da.[State] + ' ' + da.ZipCode as customerAddress
            ,de.Email as customerEmail
            ,dp.Phone as customerPhoneNumber

            ,l.CompanyName as restaurantName
            ,la.Address1 + isnull(' '+la.Address2,'') + ', ' + la.City + ', ' + la.[State] + ' ' + la.ZipCode as restaurantAddress
            ,l.CPrimaryPhone as restaurantPhoneNumber
            ,da.Latitude as pickupLatitude
            ,da.Longitude as pickupLongitude
            ,convert(varchar(8), cast(det.TargetDate as time)) as expectedDeliveryTime
        from job.jobs j
            join company.Company l on j.CompanyID = l.CompanyID
                join [crm].[CompanyAddressMapping] lam on l.CompanyID = lam.CompanyId
                join crm.Address la on lam.AddressId = la.Id
            join job.JobContactDetail dd on j.DeliveryDetailId = dd.Id
                join crm.Contact dc on dd.ContactId = dc.Id
                left outer join company.Company dcc on dc.CompanyId = dcc.CompanyID
                left outer join crm.Address da on dd.AddressId = da.Id
                left outer join crm.Phone dp on dd.PhoneId = dp.Id
                left outer join crm.Email de on dd.EmailId = de.Id
            left outer join job.TimeLine tl on j.JobID = tl.JobID and tl.TaskCode = 'de'
                left outer join task.Task det on tl.TaskID = det.TaskID
            left outer join job.ShipmentPlan sp on j.JobID = sp.JobID and sp.SequenceNo = 6 and sp.PickUp = l.CompanyID
        where l.CompanyCode = '1062tx' and det.TargetDate between DATEADD(day,0,convert(date, GETDATE())) and DATEADD(day,1,convert(date, GETDATE())) 
            and (j.DelSDoneBy = 'A' and sp.PickUp = l.CompanyID)
        """

def get_json(file):
    with open(file) as basefile:
        base = json.loads(basefile.read())
    return base

def get_cnxn():
    cnxn = pyodbc.connect(f'DRIVER={drv};SERVER={srv};DATABASE={db};UID={usr};PWD={pw}')
    return cnxn

def load_shipday_data():
    url = "https://dispatch.shipday.com/orders"
    cursor = get_cnxn().cursor().execute(sql_get_pickups())
    columns = [column[0] for column in cursor.description]
    output = []
    for row in cursor.fetchall():
        payload = dict(zip(columns, [str(v) for v in row]))
        response = requests.request("POST", url, headers=shipday_headers, data=json.dumps(payload))
        output.append(payload)
        print(response.text.encode('utf8'))
    return output