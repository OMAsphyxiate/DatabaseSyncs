import Connect as ct
from sqlalchemy import inspect, create_engine, Table, MetaData, select
import sqlanydb, sqlalchemy


def connectES():
	return sqlanydb.connect(UID=ct.SAUID,PWD=ct.SAPWD,HOST='10.20.11.250',DBN=ct.SADatabase,ENG=ct.SAServer)

hostip = '10.20.11.250'
testcreate = connectES
#<function connectES at 0x0000026C7FDA3E18>
#testcreate = connectES(hostip)
#<sqlanydb.Connection object at 0x000001CC58684748>
print(testcreate)


#Declare PG connection string
PGEngine = create_engine("postgresql+psycopg2://%s:%s@%s/%s" % (ct.PGUID, ct.PGPWD, ct.PGHost, ct.PGDatabase),isolation_level="AUTOCOMMIT")
ESEngine = sqlalchemy.create_engine('sqlalchemy_sqlany://',creator=testcreate)
MSEngine = ''


#Open connections to engines
PGConnect = PGEngine.connect() #Start connection to PG
ESConnect = ESEngine.connect() #Start connection to ES

#Create DB metadata variables
PGmetadata = MetaData()
ESmetadata = MetaData()

#Specify Table structures
PGClinic = Table('Clinic', PGmetadata, schema='Clinic', autoload=True, autoload_with=PGEngine)
ESEOD = Table('eod',ESmetadata,schema='PPM', autoload=True, autoload_with=ESEngine)

#Create Queries
PGQuery = select([PGClinic])
ESQuery = select([ESEOD])

#Store query results
PGResults = PGConnect.execute(PGQuery).fetchall()
ESResults = ESConnect.execute(ESQuery).fetchall()

for row in ESResults:
	print(row)

#Close any open connections to DBs
PGConnect.close() #Close PG Connection
ESConnect.close()