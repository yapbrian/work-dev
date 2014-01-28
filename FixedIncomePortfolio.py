from dateutil import parser
from datetime import *
import pyodbc
import sys
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

	# Load a futures curve for the current prices
	def loadShortFuturesPrices(self):
		self.shortFuturesCurvePrices = {}
		# Load the curve for pricing
		for ccy, curveName in self.ccyToFutMap.items():
			tempRiskCurve = ShortRateFuturesCurveRisk(curveName)
			tempRiskCurve.loadCurveQuotes(str(self.portDate))

			self.shortFuturesCurvePrices[curveName] = tempRiskCurve


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
				WHERE ms.securityType = '{secType}' AND tmd.PriceDate = '{sqlDate}' AND al.securityName in ({edMurexNames}) AND tdo.maturity > '{sqlDate}'
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
				# print row.currencySymbol + "," + str(optPrice) + "," + str(row.fmv) + "," + str(row.strike) + "," + row.CallPut + "," + str(row.maturity) + "," +str(row.undermat) + "," + str(row.PriceDate) + "," + str(row.totalNotional)
				# *********** NOTE THE RISK FREE RATE NEEDS TO BE CHANGED TO INTERP SWAP RATE **********
				tempFutureOpt = ShortRateFuturesOption(row.currencySymbol, str(row.undermat), row.strike, str(row.PriceDate), str(row.maturity), row.strike, 0.005, optPrice, row.totalNotional, putcall )
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
		self.thetaArray = {}
		self.vegaArray = {}

		for ccy, curve in self.ccyToFutMap.items():
			
			tempDeltaCurve = {}
			tempGammaCurve = {}
			tempThetaCurve = {}
			tempVegaCurve = {}   # This will be 2 dimensional - expiry date plus vega

			# # Fill with blank quotes for delta, gamma and theta
			# for tempBlankFut in tempRiskCurve:
			# 	try:
			# 		newDelta = tempDeltaCurve[tempBlankFut.futCode]
			# 	except KeyError:
			# 		tempDeltaCurve[tempBlankFut.futCode] = 0

			# 	try:
			# 		newGamma = tempGammaCurve[tempBlankFut.futCode]
			# 	except KeyError:
			# 		tempGammaCurve[tempBlankFut.futCode] = 0

			# 	try:
			# 		newTheta = tempThetaCurve[tempBlankFut.futCode]
			# 	except KeyError:
			# 		tempThetaCurve[tempBlankFut.futCode] = 0
			try:
				tempRiskCurve = self.shortFuturesCurvePrices[curve]
			except KeyError:
				print "Curve prices not loaded"
				continue

			# One delta generation
			for tempFut in self.shortFutPos:
				if tempFut.ccyCode == ccy:
					try:
						newDelta = tempDeltaCurve[tempFut.futCode]
					except KeyError:
						tempDeltaCurve[tempFut.futCode] = 0
						newDelta = tempDeltaCurve[tempFut.futCode]

					newDelta = newDelta + (-1)*tempFut.position*self.ccyToRiskMap[tempFut.ccyCode]
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
					
					# Delta calculation
					try:
						newDelta = tempDeltaCurve[tempFut.futCode]
					except KeyError:
						tempDeltaCurve[tempFut.futCode] = 0
						newDelta = tempDeltaCurve[tempFut.futCode]

					newDelta = newDelta + tempFutOpt.getDelta()*self.ccyToRiskMap[tempFut.ccyCode]
					tempDeltaCurve[tempFut.futCode] = newDelta

					# Gamma calculation
					try:
						newGamma = tempGammaCurve[tempFut.futCode]
					except KeyError:
						tempGammaCurve[tempFut.futCode] = 0
						newGamma = tempGammaCurve[tempFut.futCode]

					newGamma = newGamma + tempFutOpt.getGamma()*self.ccyToRiskMap[tempFut.ccyCode]
					tempGammaCurve[tempFut.futCode] = newGamma

					# Theta calculation
					try:
						newTheta = tempThetaCurve[tempFut.futCode]
					except KeyError:
						tempThetaCurve[tempFut.futCode] = 0
						newTheta = tempThetaCurve[tempFut.futCode]

					# Theta per day
					newTheta = newTheta + tempFutOpt.getTheta()*self.ccyToRiskMap[tempFut.ccyCode]
					tempThetaCurve[tempFut.futCode] = newTheta

					# Vega calculation  - 2 dimensional dictionary
					try:
						newFutVega = tempVegaCurve[tempFut.futCode]
					except KeyError:
						tempVegaCurve[tempFut.futCode] = {}
						newFutVega = tempVegaCurve[tempFut.futCode]

					# Date
					tempMatDate = tempFutOpt.matDate 

					try:
						newVega = newFutVega[tempMatDate]
					except KeyError:
						newFutVega[tempMatDate] = 0
						newVega = newFutVega[tempMatDate]

					newVega = newVega + tempFutOpt.getVega()*self.ccyToRiskMap[tempFut.ccyCode]
					newFutVega[tempMatDate] =  newVega
					tempVegaCurve[tempFut.futCode] = newFutVega


			self.futuresCurveArray[ccy] = tempRiskCurve
			self.deltaArray[ccy] = tempDeltaCurve
			self.gammaArray[ccy] = tempGammaCurve
			self.thetaArray[ccy] = tempThetaCurve
			self.vegaArray[ccy] = tempVegaCurve

		return self.futuresCurveArray

	# Shift a revalue the options - don't do anything else - for evaluating risk curve under different scenarios
	# Maybe will have a PnL impact later
	def shiftFuturesPriceCurve(self, shiftAmt = 0.01, backShiftAmt = 0.00):
		for curveName, futuresCurve in self.shortFuturesCurvePrices.items():
			# Shift for all curves
			futuresCurve.shiftPriceCurve(shiftAmt, backShiftAmt)

			# Revalue all options
			for index, tempFutOpt in enumerate(self.shortFutOptPos):
				tempFut = tempFutOpt.underlyingFut
				
				if tempFut.ccyCode == futuresCurve.ccyCode:
					newFut = futuresCurve[tempFut.futCode]
					# Have to revalue the option after setting the curve adjusted futures price
					tempFutOpt.setFutPrice(newFut.quotePrice)
					self.shortFutOptPos[index] = tempFutOpt
				# end if
			# end for
		# end for
		# Now all curves have been shifted - can recalc and redump risk curve

	# This creates and executes a bunch of sql statements to dump data into the sql database specified in
	# msSQLConnect
	# scenario code (scenCode) is applied to the data being uploaded
	def dumpRiskCurve(self, scenCode = 0):
		sqlAddValueString = ""
		delDeltaValueString = ""
		delGammaValueString = ""
		delThetaValueString = ""
		delVegaValueString = ""

		# 0 - Delta, 1 - Gamma, 2 - Theta, 3 - Vega

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
			try:
				tempDeltaCurve = self.deltaArray[ccy]
			except KeyError:
				print "KeyError for Delta with currency " + ccy
			else:
				for futCode, deltaVal in tempDeltaCurve.items():
					tempAssetID = assetIDArray[futCode]
					if sqlAddValueString == "":
						delDeltaValueString = str(tempAssetID)
						sqlAddValueString = "('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 0," + str(deltaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"
					else:
						delDeltaValueString = delDeltaValueString + "," + str(tempAssetID)
						sqlAddValueString = sqlAddValueString + ",('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 0," + str(deltaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"
			
			# create gammas insert string
			try:
				tempGammaCurve = self.gammaArray[ccy]
			except KeyError:
				print "KeyError for Gamma with currency " + ccy
			else:
				for futCode, gammaVal in tempGammaCurve.items():
					tempAssetID = assetIDArray[futCode]
					if sqlAddValueString == "":
						sqlAddValueString = "('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 1," + str(gammaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"
					else:
						sqlAddValueString = sqlAddValueString + ",('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 1," + str(gammaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"

					if delGammaValueString == "":
						delGammaValueString = str(tempAssetID)
					else:
						delGammaValueString = delGammaValueString + "," + str(tempAssetID)

			# create thetas insert string
			try:
				tempThetaCurve = self.thetaArray[ccy]
			except KeyError:
				print "KeyError for Theta with currency " + ccy 
			else:
				for futCode, thetaVal in tempThetaCurve.items():
					tempAssetID = assetIDArray[futCode]
					if sqlAddValueString == "":
						sqlAddValueString = "('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 2," + str(thetaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"
					else:
						sqlAddValueString = sqlAddValueString + ",('" + self.portDate.strftime("%Y-%m-%d") + "', '" + self.portDate.strftime("%Y-%m-%d") + "', 2," + str(thetaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"

					if delThetaValueString == "":
						delThetaValueString = str(tempAssetID)
					else:
						delThetaValueString = delThetaValueString + "," + str(tempAssetID)

			# create vega insert string
			try:
				tempVegaCurve = self.vegaArray[ccy]
			except KeyError:
				print "KeyError for Vega with currency " + ccy 
			else:
				for futCode, vegaArray in tempVegaCurve.items():
					tempAssetID = assetIDArray[futCode]
					for optMatDate, vegaVal in vegaArray.items():
						if sqlAddValueString == "":
							sqlAddValueString = "('" + self.portDate.strftime("%Y-%m-%d") + "', '" + optMatDate.strftime("%Y-%m-%d") + "', 3," + str(vegaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"
						else:
							sqlAddValueString = sqlAddValueString + ",('" + self.portDate.strftime("%Y-%m-%d") + "', '" + optMatDate.strftime("%Y-%m-%d") + "', 3," + str(vegaVal) + ","+ str(tempAssetID) + "," + str(scenCode) + ")"

					if delVegaValueString == "":
						delVegaValueString = str(tempAssetID)
					else:
						delVegaValueString = delVegaValueString + "," + str(tempAssetID)


		# Delete after all currencies processed to reduce access to the database
		if delDeltaValueString != "":
			delSqlString = """
				DELETE FROM [SHA2].[rates_data].[futures_risk_quotes] 
					WHERE save_date = '{currDate}' 
					AND quote_type = 0
					AND scenario_code = {scenarioCode}
					AND asset_id IN ({delAssetID})
				""".format(
					currDate = self.portDate.strftime("%Y-%m-%d"),
					scenarioCode = scenCode,
					delAssetID = delDeltaValueString
					)

			cur.execute(delSqlString)

		# Delete after all currencies processed to reduce access to the database
		if delGammaValueString != "":
			delSqlString = """
				DELETE FROM [SHA2].[rates_data].[futures_risk_quotes] 
					WHERE save_date = '{currDate}' 
					AND quote_type = 1
					AND scenario_code = {scenarioCode}
					AND asset_id IN ({delAssetID})
				""".format(
					currDate = self.portDate.strftime("%Y-%m-%d"),
					scenarioCode = scenCode,
					delAssetID = delGammaValueString
					)

			cur.execute(delSqlString)

		# Delete after all currencies processed to reduce access to the database
		if delThetaValueString != "":
			delSqlString = """
				DELETE FROM [SHA2].[rates_data].[futures_risk_quotes] 
					WHERE save_date = '{currDate}' 
					AND quote_type = 2
					AND scenario_code = {scenarioCode}
					AND asset_id IN ({delAssetID})
				""".format(
					currDate = self.portDate.strftime("%Y-%m-%d"),
					scenarioCode = scenCode,					
					delAssetID = delThetaValueString
					)

			cur.execute(delSqlString)

		# Delete after all currencies processed to reduce access to the database
		if delVegaValueString != "":
			delSqlString = """
				DELETE FROM [SHA2].[rates_data].[futures_risk_quotes] 
					WHERE save_date = '{currDate}' 
					AND quote_type = 3
					AND scenario_code = {scenarioCode}					
					AND asset_id IN ({delAssetID})
				""".format(
					currDate = self.portDate.strftime("%Y-%m-%d"),
					scenarioCode = scenCode,
					delAssetID = delVegaValueString
					)

			cur.execute(delSqlString)

		# Insert after all currencies proccessed to reduce access to the database
		if sqlAddValueString != "":
			sqlString = """
				INSERT INTO [SHA2].[rates_data].[futures_risk_quotes] 
					(save_date, eff_date, quote_type, risk_quote, asset_id, scenario_code)
					VALUES {valueString}
				""".format(
					valueString = sqlAddValueString
					)
		
			cur.execute(sqlString)

		# print sqlString				
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

	# Parallel shift the rates up by given amount and consequently shift prices lower
	# def shiftRatesUp(self, shiftAmt = 0.01):
	#	for tempFut in self.shortRateCurveQuotes:


####################################################################
################# Main processing module ###########################
####################################################################

runDate = ('{:%Y%m%d}'.format(datetime.now()))

if len(sys.argv) != 2:
	print "Usage: ipython FixedIncomePortfolio.py <DATE>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

updatePortfolio = FixedIncomePortfolio(sys.argv[1])
print str(runDate) + ": Loading STIR futures positions for "+ str(sys.argv[1])
updatePortfolio.loadShortFuturesPrices()
updatePortfolio.loadShortFuturesPositions()
updatePortfolio.loadShortFuturesOptPositions()
print str(runDate) + ":Calculating risk and dumping to database"
updatePortfolio.calcRiskCurve()
updatePortfolio.dumpRiskCurve()

# Scenarios -20, -10, +10, +20
updatePortfolio.shiftFuturesPriceCurve(shiftAmt=-0.20)
updatePortfolio.calcRiskCurve()
updatePortfolio.dumpRiskCurve(scenCode=1)

updatePortfolio.shiftFuturesPriceCurve(shiftAmt=+0.10)
updatePortfolio.calcRiskCurve()
updatePortfolio.dumpRiskCurve(scenCode=2)

updatePortfolio.shiftFuturesPriceCurve(shiftAmt=+0.20)
updatePortfolio.calcRiskCurve()
updatePortfolio.dumpRiskCurve(scenCode=3)

updatePortfolio.shiftFuturesPriceCurve(shiftAmt=+0.10)
updatePortfolio.calcRiskCurve()
updatePortfolio.dumpRiskCurve(scenCode=4)

print str(runDate) + ": Done for " + str(sys.argv[1])
# Exit without a problem
sys.exit(0)

