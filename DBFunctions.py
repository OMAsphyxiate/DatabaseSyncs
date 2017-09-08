import facebook, pyodbc, sys, sqlanydb, time
import psycopg2 as pg2
#sys.path.insert(0, 'C:/Users/Reaper/Dropbox/Blah/')
#sys.path.insert(0, 'C:/Users/Christian/Dropbox/Blah/')
#sys.path.insert(0, '/DabaseConnections/')
import Connect as ct
from DatabaseConnections import DatabaseConnect as dbct

#Storing Variables
GoogleList = [] #Empty list to store Google IDs
FacebookList = [] #Empty list to store Facebook IDs
ClinicList = [] #Empty list to store Clinic IDs
ClinicDict = {} #Empty dictionary to store Clinic ID and host

#Argument Variables
clincidictquery = 'SELECT "ClinicID","eaglesofthost" FROM "Clinic"."Clinic" WHERE "eaglesofthost" IS NOT NULL'
fblistquery = 'SELECT "ClinicID","facebookid" FROM "Clinic"."Clinic" WHERE "facebookid" IS NOT NULL'
gmblistquery = 'SELECT "ClinicID","googleid" FROM "Clinic"."Clinic" WHERE "googleid" IS NOT NULL'

#Populate variables for External and Internal use
#Populate ClinicDict{}
clinicdictresults = dbct.PGSelect(clincidictquery) #Pass query for ClinicDict
for key, value in clinicdictresults:
	ClinicDict[key] = value

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
		#print ('Diag Column Name: %s' % e.diag.column_name)
		#print ('Diag Contraint Name: %s' % e.diag.constraint_name)
		#print ('Diag Context: %s' % e.diag.context)
		#print ('Diag Datatype Name: %s' % e.diag.datatype_name)
		#print ('Diag Internal Position: %s' % e.diag.internal_position)
		#print ('Diag Internal Query: %s' % e.diag.internal_query)
		#print ('Diag Message Detail: %s' % e.diag.message_detail)
		#print ('Diag Message Primary: %s' % e.diag.message_primary)
		#print ('Diag SQL State: %s' % e.diag.sqlstate)
		#print ('Diag Statement Position: %s' % e.diag.statement_position)
		#print ('Diag Constraint Name: %s' % e.diag.constraint_name)
		#print ('Diag Message Hint: %s' % e.diag.message_hint)
		#print ('Diag Schema Name: %s' % e.diag.schema_name)
		#print ('Diag Severity: %s' % e.diag.severity)
		#print ('Diag Source Function: %s' % e.diag.source_function)
		#print ('Diag Source File: %s' % e.diag.source_file)
		#print ('Diag Source Line: %s' % e.diag.source_line)
		#print ('Diag Table Name: %s' % e.diag.table_name)
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