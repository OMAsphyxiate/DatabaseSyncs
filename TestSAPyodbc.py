import Connect as ct
from sqlalchemy import inspect, create_engine, Table, MetaData, select
import sqlanydb, sqlalchemy

#Declare PG connection string
#ESEngine = create_engine("sybase+pysybase://%s:%s@10.20.11.250/%s" % (ct.SAUID, ct.SAPWD,ct.SADatabase))

#Open connections to engines
#ESConnect = ESEngine.connect() #Start connection to ES
hostip = '10.20.11.250'

def connectES(host):
	conn = sqlanydb.connect(UID=ct.SAUID,PWD=ct.SAPWD,HOST=host,DBN=ct.SADatabase,ENG=ct.SAServer)
	return conn

testconn = connectES(hostip)
print(testconn)
print(sqlanydb.connect(UID=ct.SAUID,PWD=ct.SAPWD,HOST='10.20.11.250',DBN=ct.SADatabase,ENG=ct.SAServer))

#srcEngine = sqlalchemy.create_engine('sqlalchemy_sqlany://',creator=testconn)
#ESConnect = srcEngine.connect()