import facebook, sys
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

#Create database object
#database = dbct.DatabaseSync()

#Create Clinic Dictionary for Eaglesoft pulls
clinicdictresults = dbct.PGSelect(clincidictquery) #Pass query for ClinicDict
for key, value in clinicdictresults:
	ClinicDict[key] = value

#fblistresults = database.PGSelect(fblistquery)
#gmblistresults = database.PGSelect(gmblistquery)