import pyodbc, psycopg2, sys, sqlanydb
#sys.path.insert(0, 'C:/Users/Christian/Desktop/GitHub/')
sys.path.insert(0, 'C:/Users/Christian/Dropbox/Blah/')
import Connect

ClinicDict = {} #Create blank Dictionary to store query results

PostgresDatabase = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % ( #Connect to Postgres Database
    Connect.PGDatabase,
    Connect.PGUID,
    Connect.PGHost,
    Connect.PGPWD))

PGCursor = PostgresDatabase.cursor() #Create Cursor
PGCursor.execute('SELECT "ClinicID","eaglesofthost" FROM "Clinic"."Clinic" WHERE "eaglesofthost" IS NOT NULL') #Execute Query
results = PGCursor.fetchall() #Grab results from Query

for key, value in results: #For each pair of values
    ClinicDict[key] = value #Create as dictionary entry

PGCursor.close()
PostgresDatabase.close()