from dateutil import parser
from datetime import *
import pyodbc
from ShortRateFutures import *

# Inherited from futures curve for risk tracking purposes
class ShortRateFuturesCurveRisk(ShortRateFuturesCurve):
	def addRiskPosition(self, futCode, riskVal):
		for i, tempFut in enumerate(self.shortRateCurveQuotes):
			if tempFut.futCode == futCode:
				tempFut.position = riskVal
				self.shortRateCurveQuotes[i] = tempFut

class FixedIncomePortfolio(object):
	# For each currency map to the appropriate curve
	ccyToFutMap = {'USD':'USD_LIBOR_3M_FUT', 'CAD':'CAD_CDOR_3M_FUT', 'GBP':'GBP_LIBOR_3M_FUT', 'EUR':'EUR_EURIBOR_3M_FUT'}
	shortFutMXNames = {'CME EURUSD 3M':'USD', 'LIFFE EURIBOR3M':'EUR', 'LIFFE EURGBP 3M':'GBP'}

	# Init with the date
	def __init__(self, portDate):
		self.portDate = parser.parse(portDate)

	# Load some of these short futures positions
	def loadShortFuturesOptPositions(self):
		# Get options positions for futures contracts corresponding to short rate futures
		self.shortFutOptPos = []
		db = self.msSQLConnect()
		cur = db.cursor()
		# Aggregate positions for ease of use - DO NOT HAVE MATURITIES OF UNDERLYING YET IN MX
		sqlString = """
			SELECT tmd.PriceDate, tdo.maturity, sum(tdo.notional/1000000) as totalNotional, mc.currencySymbol, tdo.CallPut, tdo.strike
				FROM [SHA2].[risk].[trade_mkt_detail] tmd 
				LEFT JOIN [SHA2].[risk].[trade_detail] td ON td.tradeNum = tmd.tradeNum
				LEFT JOIN [SHA2].[risk].[trade_detail_option] tdo ON tdo.tradeNum = td.tradeNum
				LEFT JOIN [SHA2].[risk].[asset_list] al ON td.assetID = al.assetID
				LEFT JOIN [SHA2].[map].[map_currency] mc ON al.currencyID = mc.currencyID
				LEFT JOIN [SHA2].[map].[map_securitytype] ms ON al.securityTypeID = ms.securityTypeID
				WHERE ms.securityType = '{secType}' AND tmd.PriceDate = '{sqlDate}' AND al.securityName in ({edMurexNames})
				GROUP BY mc.currencySymbol, tdo.maturity, tmd.PriceDate, tdo.callPut, tdo.strike 
			""".format(
				secType = 'OPT',
				sqlDate = self.portDate.strftime('%Y-%m-%d'),
				edMurexNames = "'CME EURUSD 3M','LIFFE EURGBP 3M','LIFFE EURIBOR 3M'"
				)
		cur.execute(sqlString)

		for row in cur:
			if row.totalNotional > 0.01 or row.totalNotional < -0.01:
				print row.currencySymbol + "," + str(row.strike) + "," + row.CallPut + "," + str(row.maturity) + "," + str(row.PriceDate) + "," + str(row.totalNotional)
				# must convert the dates to strings for some reason
				#tempFuture = ShortRateFutures(row.currencySymbol, str(row.maturity), str(row.PriceDate), position = row.totalNotional)
				# tempFuture = ShortRateFutures("USD", "June 16, 2016", "November 19, 2013", 100, 100)
				# self.shortFutPos.append(tempFuture)

		return 1


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
			if row.totalNotional > 0.01 or row.totalNotional < -0.01:
				# print row.currencySymbol + "," + str(row.maturity) + "," + str(row.PriceDate) + "," + str(row.totalNotional)
				# must convert the dates to strings for some reason
				tempFuture = ShortRateFutures(row.currencySymbol, str(row.maturity), str(row.PriceDate), position = row.totalNotional)
				# tempFuture = ShortRateFutures("USD", "June 16, 2016", "November 19, 2013", 100, 100)
				self.shortFutPos.append(tempFuture)

		return self.shortFutPos
		# print(sqlString)

	# Calculate the risk across various futures curves
	def calcRiskCurve(self):
		self.riskArray = []
		for ccy, curve in self.ccyToFutMap.items():
			tempRiskCurve = ShortRateFuturesCurveRisk(curve)
			tempRiskCurve.loadCurveQuotes(str(self.portDate))
			
			for tempFut in self.shortFutPos:
				if tempFut.ccyCode == ccy:
					tempRiskCurve.addRiskPosition(tempFut.futCode, tempFut.position)

			self.riskArray.append(tempRiskCurve)

		return self.riskArray

	def dumpRiskCurve(self):
		for rateCurve in self.riskArray:
			for tempFut in rateCurve:
				print tempFut.ccyCode + " " + tempFut.futCode + ':' + str(tempFut.position)


	def msSQLConnect(self):
		dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
		try:
		    db = pyodbc.connect(dbConnectionStr, autocommit = True)
		except pyodbc.Error, err:
		    logging.warning("Database connection error. " + str(err))
		    sys.exit()
		return db