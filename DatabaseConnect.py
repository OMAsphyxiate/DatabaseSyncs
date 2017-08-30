import facebook, pyodbc, psycopg2, sys, sqlanydb
sys.path.insert(0, 'C:/Users/Christian/Desktop/GitHub/')
import Connect

def ESGrab(host,query):
	try:
		conn = sqlanydb.connect(UID=Connect.SAUID, PWD=Connect.SAPWD, HOST=host, DBN=Connect.SADatabase, ENG=Connect.SAServer)
		curs = conn.cursor()
		curs.execute(query)
		results = curs.fetchall()
		curs.close()
		conn.close()
		return results
	except:
		print("Could not connect to HOST: %s" % host)
		print(Connect.SAUID, Connect.SAPWD, Connect.SADatabase, Connect, SAServer)