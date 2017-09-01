import facebook, pyodbc, psycopg2, sys, sqlanydb
#sys.path.insert(0, 'C:/Users/Reaper/Dropbox/Blah/')
sys.path.insert(0, 'C:/Users/Christian/Dropbox/Blah/')
sys.path.insert(0, '/DabaseConnections/')
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

#Independent Functions
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
		escurs.execute(query) #Execute query argument
		esresults = escurs.fetchall() #Fetch results of query
		escurs.close() #Close Cursor
		esconn.close() #Close Connection
		return esresults #Return stored results
	except:
		print("Could not connect to HOST: %s" % host)
		#If connect fails, return host IP

def PGSelect(query=None):
	query = query or noquery
	pgconn = psycopg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" % 
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	pgcurs.execute(query) #Execute query argument
	pgresults = pgcurs.fetchall() #Fetch query results
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection
	return pgresults #Return stored results

def PGInsert(query):
	query = query or noquery #Use class query if no alternative
	pgconn = psycopg2.connect( #Establish Connection to DB
		"dbname=%s user=%s host=%s password=%s" %
		(ct.PGDatabase,ct.PGUID,ct.PGHost,ct.PGPWD))
	pgcurs = pgconn.cursor() #Create Cursor
	try: #Try INSERT Query
		pgcurs.execute(query) #Execute query argument
		pgconn.commit() #Commit INSERT
		#return print('INSERT Commited to Database')
	except:
		return print('Unable to execute INSERT query: %s' % query)
	pgcurs.close() #Close Cursor
	pgconn.close() #Close Connection

def PGInsertMany(query,list):
	query = query or noquery #Use class query if no alternative
	pgconn = psycopg2.connect( #Establish Connection to DB
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

#Populate variables for External and Internal use
#Populate ClinicDict{}
clinicdictresults = dbct.PGSelect(clincidictquery) #Pass query for ClinicDict
for key, value in clinicdictresults:
	ClinicDict[key] = value

#Internal Functions
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