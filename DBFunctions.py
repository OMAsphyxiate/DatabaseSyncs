import facebook, pyodbc, sys, sqlanydb, time
import psycopg2 as pg2
import Connect as ct
from DatabaseConnections import DatabaseConnect as dbct

#Storing Variables
GoogleList = [] #Empty list to store Google IDs
GoogleDict = {}
FacebookList = [] #Empty list to store Facebook IDs
FacebookDict = {}
ClinicList = [] #Empty list to store Clinic IDs
ClinicDict = {} #Empty dictionary to store Clinic ID and host
FacebookResults = [] #Empty list to store results

#Argument Variables
clincidictquery = 'SELECT "ClinicID","eaglesofthost" FROM "Clinic"."Clinic" WHERE "eaglesofthost" IS NOT NULL'
fblistquery = 'SELECT "facebookid" FROM "Clinic"."Clinic" WHERE "facebookid" IS NOT NULL'
gmblistquery = 'SELECT "ClinicID","googleid" FROM "Clinic"."Clinic" WHERE "googleid" IS NOT NULL'
CheckTemp = """SELECT COUNT(*) FROM pg_tables WHERE ('"' || schemaname::text || '"."' || tablename::text || '"') = '{0}'"""
CreateTemp = 'CREATE TABLE {0} AS (SELECT * FROM {1} WHERE 1 = 2)'
DropTemp = 'DROP TABLE {0}'
InsertTemp = 'INSERT INTO {0} VALUES {1}'
CountTemp = 'SELECT COUNT(*) FROM {0}'

#Populate variables for External and Internal use
#Populate ClinicDict{}
clinicdictresults = dbct.PGSelect(clincidictquery) #Pass query for ClinicDict
for key, value in clinicdictresults:
	ClinicDict[key] = value

# facebookresults = dbct.PGSelect(fblistquery) #Pass query for ClinicDict
# for key, value in facebookresults:
# 	FacebookDict[key] = value
facebooklistres = dbct.PGSelect(fblistquery)
for value in facebooklistres:
	FacebookList.extend(value)

#Independent Functions
#Grab data from Eaglesoft Server
def ESGrab(host,query=None):
	query = query or noquery #Use class query if no alternative
	try:
		esconn = sqlanydb.connect( #Establish Connection
			UID=ct.SAUID,
			PWD=ct.SAPWD,
			HOST=host,
			DBN=ct.SADatabase,
			ENG=ct.SAServer)
		escurs = esconn.cursor() #Create Cursor
		try:
			escurs.execute(query) #Execute query argument
			esresults = escurs.fetchall() #Fetch results of query
			escurs.close() #Close Cursor
			esconn.close() #Close Connection
			return esresults #Return stored results
		except(sqlanydb.Error) as se:
			print(se)
	except(sqlanydb.Error) as ce:
		print(ce)
		print("Could not connect to HOST: %s" % host)
		#If connect fails, return host IP

#Execute query on PG Database
def PGExecute(query):
	pgconn = pg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" % 
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	try: #Try INSERT Query
		pgcurs.execute(query) #Execute query argument
		pgconn.commit() #Commit INSERT
		return True
	except (pg2.Error,pg2.OperationalError,pg2.DataError,pg2.DatabaseError,pg2.ProgrammingError,pg2.IntegrityError,pg2.InterfaceError,pg2.NotSupportedError,pg2.InternalError) as e:
		print (e.pgerror)
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection

#View data from PG Server
def PGSelect(query=None):
	query = query or noquery
	pgconn = pg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" % 
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	pgcurs.execute(query) #Execute query argument
	pgresults = pgcurs.fetchall() #Fetch query results
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection
	return pgresults #Return stored results

#Insert data into PG Server
def PGInsert(query):
	query = query or noquery #Use class query if no alternative
	pgconn = pg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" %
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	try: #Try INSERT Query
		pgcurs.execute(query) #Execute query argument
		pgconn.commit() #Commit INSERT
		return True
	except (pg2.Error,pg2.OperationalError,pg2.DataError,pg2.DatabaseError,pg2.ProgrammingError,pg2.IntegrityError,pg2.InterfaceError,pg2.NotSupportedError,pg2.InternalError) as e:
		print (e.pgerror)
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection

#Insert many values itno PG (Not efficient)
def PGInsertMany(query,list):
	query = query or noquery #Use class query if no alternative
	pgconn = pg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" %
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	try: #Try INSERT Query
		pgcurs.executemany(query,) #Execute query argument
		pgconn.commit() #Commit INSERT
		#return print('INSERT Commited to Database')
	except:
		return print('Unable to execute INSERT query: %s' % query)
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection

#View data from SQL Server
def MSSelect(query=None):
	query = query or noquery #Use class query if no alternative
	msconn = pyodbc.connect( #Establish Connection with MS Database
		"Driver={%s};Server=%s;Database=%s;UID=%s;PWD=%s;" % 
		(ct.MSDriver,ct.MSServer,ct.MSDatabase,ct.MSUID,ct.MSPWD))
	mscurs = msconn.cursor() #Create Cursor
	mscurse.execute(query) #Execute Query
	msresults = mscurs.fetchall() #Fetch query results
	mscurs.close() #Close Cursor
	msconn.close() #Close Connection

#Insert Data into SQL Server
def MSInsert(query=None):
	query = query or noquery #Use class query if no alternative
	msconn = pyodbc.connect( #Establish Connection with MS Database
		"Driver={%s};Server=%s;Database=%s;UID=%s;PWD=%s;" % 
		(ct.MSDriver,ct.MSServer,ct.MSDatabase,ct.MSUID,ct.MSPWD))
	mscurs = msconn.cursor() #Create Cursor
	try:
		mscurse.execute(query) #Execute INSERT Query
	except:
		print('Unable to execute INSERT query')
	mscurs.close() #Close Cursor
	msconn.close() #Close Connection

def ListShaping(list,*args): #Add rows to list
    list.extend(args)
    return list

def TupleList(list): #Change list into String
    insertTuple = ''
    for item in list:
        insertTuple += str(tuple(item)) + ','
    insertTuple = insertTuple[:-1]  # Remove ending comma
    insertTuple = insertTuple.replace("None", "NULL")  # Replace Python 'None' with SQL NULL values in string
    return insertTuple

#Dynamic Insertion for PG for ES updates
def BuildInsertPG(GrabQuery,InsertQuery,TestRange=None):
	rangeLen = (TestRange or len(ClinicDict)+1)
	starttime = time.time()
	HostFail = []
	ResultsList = []
	for i in range(1,rangeLen): #Loop for n iterations of length of list
		hostip = ClinicDict[i] #Set variable to key value
		QueryResults = ESGrab(hostip,GrabQuery) #Set query results to variable
		if not QueryResults: #If results are empty
			HostFail.append(hostip)
			pass
		else:
			for idx, row in enumerate(QueryResults): #For each row of data
				ResultsList.append([i]) #Add list for each row with current clinic ID
				for value in row: #For each individual value in the row
					ResultsList[len(ResultsList)-1].append(value) #Add value to last list added
	insertString = ''
	for item in ResultsList:
		insertString+=str(tuple(item))+',' #Add string tuple version of list to insert string
	insertString = insertString[:-1] #Remove ending comma
	insertString = insertString.replace("None","NULL") #Replace Python 'None' with SQL NULL values in string
	injectionString = InsertQuery.format(insertString)
	try:
		ins = PGInsert(injectionString)
		if ins == True:
			print(HostFail or "Insert Success, No Failed Connections")
			print('%s execution time' % (time.time()-starttime))
		else:
			raise
	except:
		return print("Failed to INSERT")

#Dynamic Insertion for PG for ES updates, Staggered # of Clinics
def BuildInsertPGStagger(GrabQuery,InsertQuery,StartRange=None,EndRange=None):
	start = (StartRange or 1) #Starting Clinic
	end = (EndRange+1 or len(ClinicDict)+1) #Ending Clinic
	starttime = time.time()
	HostFail = []
	ResultsList = []
	for i in range(start,end): #Loop for n iterations of length of list
		hostip = ClinicDict[i] #Set variable to key value
		QueryResults = ESGrab(hostip,GrabQuery) #Set query results to variable
		if not QueryResults: #If results are empty
			HostFail.append(hostip)
			pass
		else:
			for idx, row in enumerate(QueryResults): #For each row of data
				ResultsList.append([i]) #Add list for each row with current clinic ID
				for value in row: #For each individual value in the row
					ResultsList[len(ResultsList)-1].append(value) #Add value to last list added
	insertString = ''
	for item in ResultsList:
		insertString+=str(tuple(item))+',' #Add string tuple version of list to insert string
	insertString = insertString[:-1] #Remove ending comma
	insertString = insertString.replace("None","NULL") #Replace Python 'None' with SQL NULL values in string
	injectionString = InsertQuery.format(insertString)
	try:
		ins = PGInsert(injectionString)
		if ins == True:
			print(HostFail or "Insert Success, No Failed Connections")
			print('%s execution time' % (time.time()-starttime))
		else:
			raise
	except:
		return print("Failed to INSERT")

#Building and Inserting data via temporary tables
def BuildInsertPGTemp(GrabQuery,TempTable,CentralTable,TempCompare,StartRange=None,EndRange=None):
	start = (StartRange or 1) #Starting Clinic
	end = (EndRange or len(ClinicDict))+1 #Ending Clinic
	tempCheck = CheckTemp.format(TempTable)
	tempDrop = DropTemp.format(TempTable) #Check if temporary Table exists
	tempCreate = CreateTemp.format(TempTable,CentralTable)
	tempCounter = CountTemp.format(TempTable)
	if PGSelect(tempCheck) == 1:
		PGExecute(tempDrop)
	starttime = time.time()
	HostFail = []
	ResultsList = []
	for i in range(start,end): #Loop for n iterations of length of list
		hostip = ClinicDict[i] #Set variable to key value
		QueryResults = ESGrab(hostip,GrabQuery) #Set query results to variable
		if not QueryResults: #If results are empty
			HostFail.append(hostip)
			pass
		else:
			for idx, row in enumerate(QueryResults): #For each row of data
				ResultsList.append([i]) #Add list for each row with current clinic ID
				for value in row: #For each individual value in the row
					ResultsList[len(ResultsList)-1].append(value) #Add value to last list added
	insertString = ''
	for item in ResultsList:
		insertString+=str(tuple(item))+',' #Add string tuple version of list to insert string
	insertString = insertString[:-1] #Remove ending comma
	insertString = insertString.replace("None","NULL") #Replace Python 'None' with SQL NULL values in string
	injectionString = InsertTemp.format(TempTable,insertString) #Group INSERT Query + values
	PGExecute(tempCreate) #Create Temporary Table using existing table
	PGInsert(injectionString) #Insert ES data into Temporary Table
	try:
		ins = PGInsert(TempCompare) 
		if ins == True:
			print(HostFail or "Insert Success, No Failed Connections")
			print('%s execution time' % (time.time()-starttime))
		else:
			raise
	except:
		return print("Failed to INSERT")
	#print(PGSelect(tempCounter))
	PGExecute(tempDrop)

#Test for failing Inserts
def BuildInsertTest(GrabQuery,InsertQuery,StartRange=None,EndRange=None):
	start = (StartRange or 1)
	end = (EndRange or len(ClinicDict)+1)
	starttime = time.time()
	HostFail = []
	ResultsList = []
	for i in range(start,end): #Loop for n iterations of length of list
		hostip = ClinicDict[i] #Set variable to key value
		QueryResults = ESGrab(hostip,GrabQuery) #Set query results to variable
		if not QueryResults: #If results are empty
			HostFail.append(hostip)
			pass
		else:
			for idx, row in enumerate(QueryResults): #For each row of data
				ResultsList.append([i]) #Add list for each row with current clinic ID
				for value in row: #For each individual value in the row
					ResultsList[len(ResultsList)-1].append(value) #Add value to last list added
	insertString = ''
	for item in ResultsList:
		insertString+=str(tuple(item))+',' #Add string tuple version of list to insert string
	insertString = insertString[:-1] #Remove ending comma
	insertString = insertString.replace("None","NULL") #Replace Python 'None' with SQL NULL values in string
	injectionString = InsertQuery.format(insertString)
	try:
		ins = PGInsert(injectionString)
		if ins == True:
			print(HostFail or "Insert Success, No Failed Connections")
			print('%s execution time' % (time.time()-starttime))
		else:
			raise
	except:
		return print("Testing Failed")
		#print("Failed to INSERT")
		#print(injectionString)

def TempTableTest(GrabQuery,TempTable,CentralTable,StartRange=None,EndRange=None):
	start = (StartRange or 1) #Starting Clinic
	end = (EndRange or len(ClinicDict))+1 #Ending Clinic
	tempCheck = CheckTemp.format(TempTable) #Check if temporary Table exists
	tempDrop = DropTemp.format(TempTable) #Drop temp table
	tempCreate = CreateTemp.format(TempTable,CentralTable) #Create temp table
	tempCounter = CountTemp.format(TempTable) #Count # of rows on temp table
	checker = PGSelect(tempCheck)
	#print(checker)
	if checker[0][0] == 1:
		PGExecute(tempDrop)
	starttime = time.time()
	HostFail = []
	ResultsList = []
	for i in range(start,end): #Loop for n iterations of length of list
		hostip = ClinicDict[i] #Set variable to key value
		QueryResults = ESGrab(hostip,GrabQuery) #Set query results to variable
		if not QueryResults: #If results are empty
			HostFail.append(hostip)
			pass
		else:
			for idx, row in enumerate(QueryResults): #For each row of data
				ResultsList.append([i]) #Add list for each row with current clinic ID
				for value in row: #For each individual value in the row
					ResultsList[len(ResultsList)-1].append(value) #Add value to last list added
	insertString = ''
	for item in ResultsList:
		insertString+=str(tuple(item))+',' #Add string tuple version of list to insert string
	insertString = insertString[:-1] #Remove ending comma
	insertString = insertString.replace("None","NULL") #Replace Python 'None' with SQL NULL values in string
	injectionString = InsertTemp.format(TempTable,insertString) #Group INSERT Query + values
	PGExecute(tempCreate) #Create Temporary Table using existing table
	PGInsert(injectionString) #Insert ES data into Temporary Table
	print('%s execution time' % (time.time()-starttime))

#Internal Functions
#Check to see if all ES Clinics can be connected
def ESCheck():
	StatusCheck = []
	for i in range(1,43): #len(ClinicDict)+1
		try:
		    esconn = sqlanydb.connect( #Establish Connection
		        UID=ct.SAUID,
		        PWD=ct.SAPWD,
		        HOST=ClinicDict[i],
		        DBN=ct.SADatabase,
		        ENG=ct.SAServer)
		    esconn.close() #Close Connection
		    StatusCheck.append("Clinic %s: Connection Success" % i)
		except:
		    StatusCheck.append("Could not connect to Clinic %s at HOST: %s" % (i,ClinicDict[i]))
	return StatusCheck