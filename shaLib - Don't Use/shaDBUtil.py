import pyodbc
import logging

def MSSQLConnect(serv):
	CONNECTIONSTRING = ''
	if serv == 0: #prod server 
		CONNECTIONSTRING = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	elif serv == 1: #dev server
		CONNECTIONSTRING = "DRIVER={SQL Server};SERVER=DVIDBSQL01\DEV2008;DATABASE=SHA2;Trusted_Connection=yes"
	else:
		logging.error("Unsupported server selection")
		exit()

	try:
	    con = pyodbc.connect(CONNECTIONSTRING, autocommit = True)
	except pyodbc.Error, err:
	    logging.error("Database connection error. " + str(err))
	    exit()
	return con

def ExecuteSelectStmt(sqlStr, serv = 0):
	con = MSSQLConnect(serv)
	cur = con.cursor()
	try:
		cur.execute(sqlStr)
	except Exception as err:
		logging.error("%s", sqlStr)
		logging.warning("Query Failed: %s", err)
		return False

	resultSet = cur.fetchall()
	if not resultSet:
		logging.error("%s", sqlStr)
		logging.warning("No results from db")
		return False
	else:
		return resultSet

def UpdateInsertStmt(sqlStr, serv = 0):
	con = MSSQLConnect(serv)
	cur = con.cursor()
	try:
		cur.execute(sqlStr)
		return True
	except Exception as err:
		logging.error("%s", sqlStr)
		logging.warning("Failed to update/insert to db: %s", err)
		return False