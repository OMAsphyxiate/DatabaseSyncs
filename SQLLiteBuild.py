import Connect as ct
from sqlalchemy import inspect,create_engine #Import ability to create a database engine to work with

#Create Postgres engine with connection string
engine = create_engine("postgresql+psycopg2://%s:%s@%s/%s" % (ct.PGUID, ct.PGPWD, ct.PGHost, ct.PGDatabase),isolation_level="AUTOCOMMIT")

#Connect to the DB using the created engine variable
connection = engine.connect()
#Submit query against the connected DB
stmt = 'SELECT * FROM "Clinic"."Clinic" ORDER BY "ClinicID"'
#Set cursor for the results
result_proxy = connection.execute(stmt)
#Store all the cursor's values into a results variable
results = result_proxy.fetchall()

#for row in results:
#	print(row.keys()) #Prints column names for each row


from sqlalchemy import Table, MetaData

metadata = MetaData()

clinic = Table('Clinic', metadata, schema='Clinic', autoload=True, autoload_with=engine)

sqlquery = select([clinic])

print(sqlquery)

sqlresults = connection.execute(sqlquery).fetchall()

