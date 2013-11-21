from dateutil import parser
import datetime
import pyodbc

def loadCurveQuotes(currDate):
	db = msSQLConnect()
	cur = db.cursor()
	currDate = parser.parse(currDate)
	qryString = """
	SELECT * FROM [SHA2].[rates_data].[futures_quotes] fq 
		LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id 
		LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
		WHERE att.asset_name = '{curveName}' AND fq.save_date = '{currDateOut}'
		""".format(
			curveName = "USD_LIBOR_3M_FUT",
			currDateOut = currDate.strftime('%Y-%m-%d')
			)
	print(qryString)
	return qryString
	


def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
	    db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
	    logging.warning("Database connection error. " + str(err))
	    sys.exit()
	return db