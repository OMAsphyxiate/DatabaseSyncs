import facebook
#sys.path.insert(0, 'C:/Users/Reaper/Dropbox/Blah/')
sys.path.insert(0, 'C:/Users/Christian/Dropbox/Blah/')
import Connect as ct
import DatabaseConnect as dbct

#Storing Variables
GoogleList = [] #Empty list to store Google IDs
UserList = [] #Empty list to store Facebook IDs
ClinicList = [] #Empty list to store Clinic IDs
ClinicDict = {} #Empty dictionary to store Clinic ID and host

#Argument Variables
clincidictquery = 'SELECT "ClinicID","eaglesofthost" FROM "Clinic"."Clinic" WHERE "eaglesofthost" IS NOT NULL'

#Create database object
database = DatabaseSync()

#Create Clinic Dictionary for Eaglesoft pulls
clinicdictresults = database.PGSelect(clincidictquery) #Pass query for ClinicDict
for key, value in clinicdictresults:
	ClinicDict[key] = value