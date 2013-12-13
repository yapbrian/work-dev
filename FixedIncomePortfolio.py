from dateutil import parser
from datetime import *
import pyodbc
from ShortRateFutures import *
from ShortRateFuturesOption import *

# Inherited from futures curve for risk tracking purposes
class ShortRateFuturesCurveRisk(ShortRateFuturesCurve):
	# Placeholder
	def emptyFunc(self):
		return 0
	# def addRiskPosition(self, futCode, riskVal):
	# 	for i, tempFut in enumerate(self.shortRateCurveQuotes):
	# 		if tempFut.futCode == futCode:
	# 			tempFut.position = tempFut.position + riskVal
	# 			self.shortRateCurveQuotes[i] = tempFut

class FixedIncomePortfolio(object):
	# For each currency map to the appropriate curve
	ccyToFutMap = {'USD':'USD_LIBOR_3M_FUT', 'CAD':'CAD_CDOR_3M_FUT', 'GBP':'GBP_LIBOR_3M_FUT', 'EUR':'EUR_EURIBOR_3M_FUT'}
	shortFutMXNames = {'CME EURUSD 3M':'USD', 'LIFFE EURIBOR3M':'EUR', 'LIFFE EURGBP 3M':'GBP'}
	ccyToRiskMap = {'USD':25, 'CAD':25, 'EUR':25, 'GBP':12.5}

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
			SELECT tmd.PriceDate, sum(tmd.FairMarketValue) as fmv, tdo.maturity, td.maturity as undermat, sum(tdo.notional/1000000) as totalNotional, mc.currencySymbol, tdo.CallPut, tdo.strike
				FROM [SHA2].[risk].[trade_mkt_detail] tmd 
				LEFT JOIN [SHA2].[risk].[trade_detail] td ON td.tradeNum = tmd.tradeNum
				LEFT JOIN [SHA2].[risk].[trade_detail_option] tdo ON tdo.tradeNum = td.tradeNum
				LEFT JOIN [SHA2].[risk].[asset_list] al ON td.assetID = al.assetID
				LEFT JOIN [SHA2].[map].[map_currency] mc ON al.currencyID = mc.currencyID
				LEFT JOIN [SHA2].[map].[map_securitytype] ms ON al.securityTypeID = ms.securityTypeID
				WHERE ms.securityType = '{secType}' AND tmd.PriceDate = '{sqlDate}' AND al.securityName in ({edMurexNames}) AND tdo.maturity >= '{sqlDate}'
				GROUP BY mc.currencySymbol, tdo.maturity, td.maturity, tmd.PriceDate, tdo.callPut, tdo.strike 
			""".format(
				secType = 'OPT',
				sqlDate = self.portDate.strftime('%Y-%m-%d'),
				edMurexNames = "'CME EURUSD 3M','LIFFE EURGBP 3M','LIFFE EURIBOR 3M'"
				)
		cur.execute(sqlString)

		for row in cur:
			if row.totalNotional > 0.01 or row.totalNotional < -0.01:
				# calculate the price
				bpVal = self.ccyToRiskMap[row.currencySymbol]
				optPrice = row.fmv / row.totalNotional / bpVal / 100.0
				putcall = 0 if row.CallPut == "call" else 1
				print row.currencySymbol + "," + str(optPrice) + "," + str(row.fmv) + "," + str(row.strike) + "," + row.CallPut + "," + str(row.maturity) + "," +str(row.undermat) + "," + str(row.PriceDate) + "," + str(row.totalNotional)
				# *********** NOTE THE RISK FREE RATE NEEDS TO BE CHANGED TO INTERP SWAP RATE **********
				tempFutureOpt = ShortRateFuturesOption(row.currencySymbol, str(row.undermat), row.strike, str(row.PriceDate), str(row.maturity), row.strike, 0.01, optPrice, row.totalNotional, putcall )
				# tempFuture = ShortRateFutures(row.currencySymbol, str(row.maturity), str(row.PriceDate), position = row.totalNotional)
				# tempFuture = ShortRateFutures("USD", "June 16, 2016", "November 19, 2013", 100, 100)
				self.shortFutOptPos.append(tempFutureOpt)

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
	# riskArray is an array of currencies each with a riskCurve which is a curve of short futures positions (plus options)
	# Without futures prices (call a different function)
	def calcRiskCurve(self):
		# Each array holds an array of dictionaries with deltas/gammas corresponding to the listed options
		# futuresCurveArray takes the current price for each future at which risk is evaluated
		self.futuresCurveArray = {}
		self.deltaArray = {}
		self.gammaArray = {}

		for ccy, curve in self.ccyToFutMap.items():
			# Load the curve for pricing
			tempRiskCurve = ShortRateFuturesCurveRisk(curve)
			tempRiskCurve.loadCurveQuotes(str(self.portDate))
			
			tempDeltaCurve = {}
			tempGammaCurve = {}

			# One delta generation
			for tempFut in self.shortFutPos:
				if tempFut.ccyCode == ccy:
					try:
						newDelta = tempDeltaCurve[tempFut.futCode]
					except KeyError:
						tempDeltaCurve[tempFut.futCode] = 0
						newDelta = tempDeltaCurve[tempFut.futCode]

					newDelta = newDelta + tempFut.position*self.ccyToRiskMap[tempFut.ccyCode]
					tempDeltaCurve[tempFut.futCode] = newDelta

			# Options delta generation
			for tempFutOpt in self.shortFutOptPos:
				tempFut = tempFutOpt.underlyingFut
				if tempFut.ccyCode == ccy:
					newFut = tempRiskCurve[tempFut.futCode]
					oldPrice = tempFutOpt.price
					# Have to revol the option after setting futures price
					tempFutOpt.setFutPrice(newFut.quotePrice)
					tempFutOpt.price = oldPrice
					
					try:
						newDelta = tempDeltaCurve[tempFut.futCode]
					except KeyError:
						tempDeltaCurve[tempFut.futCode] = 0
						newDelta = tempDeltaCurve[tempFut.futCode]

					newDelta = newDelta + tempFutOpt.getDelta()*self.ccyToRiskMap[tempFut.ccyCode]
					tempDeltaCurve[tempFut.futCode] = newDelta

			self.futuresCurveArray[ccy] = tempRiskCurve
			self.deltaArray[ccy] = tempDeltaCurve

		return self.futuresCurveArray

	def dumpRiskCurve(self):
		sqlValueString = ""
		for ccy, assetName in self.ccyToFutMap.items():
			# query the asset IDs and make a dictionary
			assetIDArray = {}
			# Query risk report database for current positions - can change this but have to change parsing
			db = self.msSQLConnect()
			cur = db.cursor()
			# Also aggregates positions for ease of use
			sqlString = """
				SELECT fq.futures_code, fq.asset_id
					FROM [SHA2].[rates_data].[futures_quotes] fq 
					LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id
					LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
					WHERE att.asset_type_name = '{assetTypeName}' AND fq.save_date = '{sqlDate}'
				""".format(
					assetTypeName = assetName,
					sqlDate = self.portDate.strftime('%Y-%m-%d')
					)
			cur.execute(sqlString)

			# populate the dictionary
			for row in cur:
				assetIDArray[row.futures_code] = row.asset_id

			# create deltas insert string
			tempDeltaCurve = self.deltaArray[ccy]

			for futCode, deltaVal in tempDeltaCurve.items():
				tempAssetID = assetIDArray[futCode]
				if sqlValueString == "":
					sqlValueString = "('" + futCode + ",'" + self.portDate.strftime("%Y-%m-%d") + "', '0d', 0," + str(deltaVal) + ","+ str(tempAssetID) + ")"
				else:
					sqlValueString = sqlValueString + ",('" + futCode + ",'"  + self.portDate.strftime("%Y-%m-%d") + "', '0d', 0," + str(deltaVal) + ","+ str(tempAssetID) + ")"
		
		print sqlValueString				
		# for ccy, deltaCurve in self.deltaArray.items():
		#	for futCode, tempDelta in deltaCurve.items():
		#		print futCode + ":" + str(tempDelta)


	def msSQLConnect(self):
		dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
		try:
		    db = pyodbc.connect(dbConnectionStr, autocommit = True)
		except pyodbc.Error, err:
		    logging.warning("Database connection error. " + str(err))
		    sys.exit()
		return db