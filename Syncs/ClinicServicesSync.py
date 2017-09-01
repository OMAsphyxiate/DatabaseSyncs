from DatabaseSyncs import DBFunctions as dbf

ListofList = []
HostFail = []
sqlquery = """
SELECT 
service_code,
IFNULL(ada_code,'',ada_code),
description,
CAST(IFNULL(service_type_id,'0',service_type_id) AS INT),
impacted_area,
REPLACE(IFNULL(smart_code1,'NULL',smart_code1),'''',''),
REPLACE(IFNULL(smart_code2,'NULL',smart_code2),'''',''),
REPLACE(IFNULL(smart_code3,'NULL',smart_code3),'''',''),
REPLACE(IFNULL(smart_code4,'NULL',smart_code4),'''',''),
REPLACE(IFNULL(smart_code5,'NULL',smart_code5),'''',''),
CAST(ISNULL(sequence,0,sequence) AS INT),
CAST(ISNULL(fee,0,fee) AS NUMERIC(16,2))
FROM services
"""

for i in range(1,len(dbf.ClinicDict)+1): #Loop while in range of max clinic #len(dbf.ClinicDict)+1
    hostip = dbf.ClinicDict[i] #Set IP Variable
    QueryResults = dbf.ESGrab(hostip,sqlquery)
    if not QueryResults:
        HostFail.append(hostip)
        pass
    else:
        for idx, row in enumerate(QueryResults): #For each row of data
            ListofList.append([i]) #add list for each row with clinic ID
            for value in row: #For each value in the row's Tuple
                ListofList[len(ListofList)-1].append(value) #Append to last list added

dataText = ''
for item in ListofList:
    dataText+=str(tuple(item))+','
dataText = dataText[:-1]
insertString = 'INSERT INTO "Clinic"."Services" VALUES {0} ON CONFLICT (clinicid, servicecode) DO NOTHING'.format(dataText) #Create row insert string
try:
    dbf.PGInsert(insertString) #Execute query
except:
    print("Couldn't INSERT row: ") #Error
    print(insertString) #Error
print(HostFail or "No failures")
