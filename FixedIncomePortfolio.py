from dateutil import parser
from datetime import *
import pyodbc
from ShortRateFutures import *

class FixedIncomePortfolio(object):
	# Init with the date
	def __init__(self, portDate):
		self.portDate = parser.parse(portDate)

	# Query some table (either risk report or whatever for short futures positions)
	def loadShortFuturesPositions(self):
		# Make a list storing them in a list of short rate futures objects
		self.shortFutPos = []
		# Query risk report database for current positions - can change this but have to change parsing
		db = self.msSQLConnect()
		cur = db.cursor()
		# Also aggregates positions for ease of use
		sqlString = """
			SELECT tmd.PriceDate, td.maturity, sum(td.notional) as totalNotional, mc.currencySymbol
				FROM [SHA2].[risk].[trade_mkt_detail] tmd 
				LEFT JOIN [SHA2].[risk].[trade_detail] td ON td.tradeNum = tmd.tradeNum
				LEFT JOIN [SHA2].[risk].[asset_list] al ON td.assetID = al.assetID
				LEFT JOIN [SHA2].[map].[map_currency] mc ON al.currencyID = mc.currencyID
				LEFT JOIN [SHA2].[map].[map_securitytype] ms ON al.securityTypeID = ms.securityTypeID
				WHERE ms.securityType = '{secType}' AND tmd.PriceDate = '{sqlDate}'
				GROUP BY mc.currencySymbol, td.maturity, tmd.PriceDate
			""".format(
				secType = 'SFUT',
				sqlDate = self.portDate.strftime('%Y-%m-%d')
				)
		cur.execute(sqlString)

		for row in cur:
			if row.totalNotional > 0.5 or row.totalNotional < -0.5:
				#print row.currencySymbol + "," + str(row.maturity) + "," + str(row.PriceDate) + "," + str(row.totalNotional)
				# must convert the dates to strings for some reason
				tempFuture = ShortRateFutures(row.currencySymbol, str(row.maturity), str(row.PriceDate), position = row.totalNotional)
				# tempFuture = ShortRateFutures("USD", "June 16, 2016", "November 19, 2013", 100, 100)
				self.shortFutPos.append(tempFuture)

		return self.shortFutPos
		# print(sqlString)

	def msSQLConnect(self):
		dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
		try:
		    db = pyodbc.connect(dbConnectionStr, autocommit = True)
		except pyodbc.Error, err:
		    logging.warning("Database connection error. " + str(err))
		    sys.exit()
		return db