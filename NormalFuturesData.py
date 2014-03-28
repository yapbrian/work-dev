from dateutil import parser
import datetime
import pyodbc
import pandas.io.sql as psql

def loadCurveNormQuotes(asOfDate, curveName):
	db = msSQLConnect()
	cur = db.cursor()
	asOfDate = parser.parse(asOfDate)
	qryString = """
	SELECT save_date, asset_id, rate_quote FROM [SHA2].[rates_data].[futures_norm_quotes] fq 
		LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id 
		LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
		WHERE att.asset_name = '{sqlCurveName}' AND fq.save_date >= '{currDateOut}'
		ORDER BY fq.save_date
		""".format(
			sqlCurveName = curveName,
			currDateOut = asOfDate.strftime('%Y-%m-%d')
			)
	
	normFutDF = psql.frame_query(sql,db)
	normFutDF['save_date'] = normFutDF['save_date'].apply(parser.parse)
	pivotNormFutDF = normFutDF.pivot('save_date','asset_id','rate_quote')

	return qryString
	


def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
	    db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
	    logging.warning("Database connection error. " + str(err))
	    sys.exit()
	return db


