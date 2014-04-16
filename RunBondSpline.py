import GovernmentBonds as gb
import FedTreasurySpline as fts
import pandas as pd
import findates as fd 	# Importa user defined library for financial date processing
import pyodbc
import sys
from datetime import *
from scipy.optimize import leastsq
from dateutil import parser
# import pdb


# Run's the spline excluding the first off the run and the on the run benchmark
def runBondSpline(asOfDate, ccyName, bondTypeName, bondCurveName, maturityCutoff, paramEst, onTheRunExcl = 2, offTheRunIncl = 5):
	bondCurveClass = gb.GovernmentBondCurve(ccyName,bondCurveName)

	onTheRunDictionary = {}

	if onTheRunExcl > 0:
		bondCurveClass.loadBondBmkQuotes(asOfDate)

		for tempCUSIP, tempBondPricer in bondCurveClass.bondBmkCurveQuotes.items():
			tempBond = tempBondPricer.bondObj
			onTheRunDictionary[tempCUSIP] = tempBond.maturityDt


	if onTheRunExcl > 1:
		bondCurveClass.loadBondOffTheRunQuotes(asOfDate,onTheRunExcl-1)

		for tempCUSIP, tempBondPricer in bondCurveClass.bondOffTheRunQuotes.items():
			tempBond = tempBondPricer.bondObj
			onTheRunDictionary[tempCUSIP] = tempBond.maturityDt

	bondCurveClass.loadBondOffTheRunQuotes(asOfDate,offTheRunIncl+onTheRunExcl-1)

	priceArray = []
	tempSeriesArray = []
	# Aggregate the bond cashflow data
	for tempCUSIP, tempBondPricer in bondCurveClass.bondOffTheRunQuotes.items():
		tempBond = tempBondPricer.bondObj
		
		if tempBond.CUSIP not in onTheRunDictionary:
			if tempBond.maturityDt > maturityCutoff:
				tempSeries = tempBond.generateCashflows()
				tempSeriesArray.append(tempSeries)
				priceArray.append(tempBondPricer.price+tempBondPricer.accrInt)
				# print str(tempBondPricer.calcPriceTrueYield(0.02)) + " " + tempBond.CUSIP

	bondCal = fd.get_calendar(fdCcyMap[ccyName])
	settleDate = fd.rolldate(asOfDate + timedelta(days=tempBond.settleDelay), bondCal, "follow")

	fedSpline = fts.FedTreasurySpline(settleDate, tempSeriesArray)
	#DEBUG
	#pdb.set_trace()
	#DEBUG
	successFlag = False
	numTries = 0

	while successFlag == False and numTries < 20:
		try:
			fitParams = leastsq(fedSpline.getResidual, paramEst, args=(priceArray), maxfev=10000)
			successFlag = True
		except OverflowError:
			paramEst = [x/-2.0 for x in paramEst]
			print "OVERFLOW ERROR TRYING " + str(paramEst)
			numTries = numTries + 1
			pass

	if successFlag == False:
		return [0,0,0,0,0,0]
	else:
		return fitParams[0]


def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
		db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
		logging.warning("Database connection error. " + str(err))
		sys.exit()
	return db


def getStoredParams(asOfDate, bondTypeName):
	strSql = """
	SELECT b0.param_quote as beta0, b1.param_quote as beta1, b2.param_quote as beta2, b3.param_quote as beta3, t1.param_quote as tau1, t2.param_quote as tau2
		FROM [SHA2].[rates_data].[bond_tsm_param_quotes] b0
		LEFT JOIN [SHA2].[rates_data].[bond_tsm_param_quotes] b1 ON b0.asset_id = b1.asset_id
		LEFT JOIN [SHA2].[rates_data].[bond_tsm_param_quotes] b2 ON b0.asset_id = b2.asset_id
		LEFT JOIN [SHA2].[rates_data].[bond_tsm_param_quotes] b3 ON b0.asset_id = b3.asset_id
		LEFT JOIN [SHA2].[rates_data].[bond_tsm_param_quotes] t1 ON b0.asset_id = t1.asset_id
		LEFT JOIN [SHA2].[rates_data].[bond_tsm_param_quotes] t2 ON b0.asset_id = t2.asset_id
		LEFT JOIN [SHA2].[rates_data].[asset_table] at ON b0.asset_id = at.asset_id
		WHERE at.asset_name = '{sqlBondTypeName}' AND b0.param_id = 0 AND b1.param_id = 1 AND b2.param_id = 2 AND b3.param_id = 3 AND t1.param_id = 4 AND t2.param_id = 5
		AND b0.save_date = '{sqlAsOfDate}' AND b1.save_date = '{sqlAsOfDate}' AND b2.save_date = '{sqlAsOfDate}' AND b3.save_date = '{sqlAsOfDate}'
		AND t1.save_date = '{sqlAsOfDate}' AND t2.save_date = '{sqlAsOfDate}'
		""".format(
			sqlBondTypeName = bondTypeName,
			sqlAsOfDate = asOfDate.strftime("%Y-%m-%d")
			)

	db = msSQLConnect()
	cur = db.cursor()

	cur.execute(strSql)

	b0 = 13.637
	b1 = -13.625615191
	b2 = -39.65844
	b3 = 19.34625
	t1 = 92.66853
	t2 = 66.80389

	# Assign parameters
	for row in cur:
		if row.beta0 != 0.0:
			b0 = row.beta0
		if row.beta1 != 0.0:
			b1 = row.beta1
		if row.beta2 != 0.0:
			b2 = row.beta2
		if row.beta3 != 0.0:
			b3 = row.beta3
		if row.tau1 != 0.0:
			t1 = row.tau1
		if row.tau2 != 0.0:
			t2 = row.tau2

	return [b0,b1,b2,b3,t1,t2]


def putParams(saveDate, bondTypeName, paramsToUpload):

	strSql = "SELECT at.asset_id FROM [SHA2].[rates_data].[asset_table] at WHERE at.asset_name = '{sqlBondTypeName}'".format(sqlBondTypeName = bondTypeName)

	db = msSQLConnect()
	cur = db.cursor()
	cur.execute(strSql)

	# should only be one
	for row in cur:
		bondAssetID = row.asset_id

	sqlValues = ""
	sqlDelValues = ""
	# assume in correct order
	for index,tempParam in enumerate(paramsToUpload):
		if sqlValues == "":
			sqlValues = "({sqlAssetID},'{sqlSaveDate}',{sqlParamID},{sqlParamQuote})".format(sqlAssetID = bondAssetID, sqlSaveDate = saveDate.strftime("%Y-%m-%d"), sqlParamID = index, sqlParamQuote=tempParam)
			delSqlValues = str(index)
		else:
			sqlValues = sqlValues + ",({sqlAssetID},'{sqlSaveDate}',{sqlParamID},{sqlParamQuote})".format(sqlAssetID = bondAssetID, sqlSaveDate = saveDate.strftime("%Y-%m-%d"), sqlParamID = index, sqlParamQuote=tempParam)
			delSqlValues = delSqlValues + "," + str(index)

	strSql = "DELETE FROM [SHA2].[rates_data].[bond_tsm_param_quotes] WHERE save_date = '{sqlSaveDate}' AND asset_id = {sqlAssetID} AND param_id IN ({sqlDelValues})".format(sqlSaveDate = saveDate.strftime("%Y-%m-%d"), sqlAssetID = bondAssetID, sqlDelValues=delSqlValues)

	cur.execute(strSql)

	strSql = "INSERT INTO [SHA2].[rates_data].[bond_tsm_param_quotes] (asset_id, save_date, param_id, param_quote) VALUES " + sqlValues

	cur.execute(strSql)
	return strSql


####################################################################
################# Main processing module ###########################
####################################################################


if len(sys.argv) < 2:
	print "Usage: ipython RatesBondSpline.py <DATE> <DATEEND> <CCY>"
	print "<DATE>,<DATEEND> is in YYYYMMDD format"
	print "<CCY> is a three letter currency code (supported: USD,CAD,GBP,EUR)"
	sys.exit(1)

fdCcyMap = {"USD":"us","CAD":"ca","GBP":"uk","EUR":"de"}
ccyNameArray = ["USD","CAD","GBP","EUR"]
bondTypeNameArray = ["USD_CASH_BOND_GOVT","CAD_CASH_BOND_GOVT","GBP_CASH_BOND_GOVT","DEM_CASH_BOND_GOVT"]
bondCurveNameArray = ["USD_CASH_BOND_GOVT_BMK","CAD_CASH_BOND_GOVT_BMK","GBP_CASH_BOND_GOVT_BMK","DEM_CASH_BOND_GOVT_BMK"]
maturityCutoffArray = ["2015-06-01","2016-04-01","2015-01-01","2016-04-01"]
onTheRunExclArray = [2,1,1,1]
offTheRunInclArray = [5,5,5,5]


if len(sys.argv) == 2:
		
	asOfDate = parser.parse(sys.argv[1])

	for index,ccyName in enumerate(ccyNameArray):
		tempCal = fd.get_calendar(fdCcyMap[ccyName])
		prevDate = fd.rolldate(asOfDate-timedelta(days=1), tempCal, "previous")

		paramEst = getStoredParams(prevDate,bondTypeNameArray[index])

		maturityCutoff = parser.parse(maturityCutoffArray[index])

		paramResult = runBondSpline(asOfDate, ccyName, bondTypeNameArray[index], bondCurveNameArray[index], maturityCutoff, paramEst,onTheRunExclArray[index],offTheRunInclArray[index])
		# paramEst = [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144] # Run the last one [0.1,0.1,0.1,0.1,10,10] [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144]

		tempSql = putParams(asOfDate, bondTypeNameArray[index], paramResult)
		print asOfDate.strftime("%Y-%m-%d") + " " + ccyName + " DONE WITH " + str(onTheRunExclArray[index]) + " ON THE RUNS EXCL AND " + str(offTheRunInclArray[index]) + " OFF THE RUNS"

if len(sys.argv) == 4:
	startDate = parser.parse(sys.argv[1])
	endDate = parser.parse(sys.argv[2])
	ccyName = sys.argv[3]

	ccyName = ccyName.upper()

	currIndex = -1

	for index, item in enumerate(ccyNameArray):
		if item == ccyName:
			currIndex = index

	if currIndex == -1:
		print "Could not find currency " + ccyName
		sys.exit(1)

	tempCal = fd.get_calendar(fdCcyMap[ccyName])

	currDate = fd.rolldate(startDate, tempCal,"follow")
	prevDate = fd.rolldate(startDate-timedelta(days=1), tempCal, "previous")

	while currDate < endDate:
		paramEst = getStoredParams(prevDate,bondTypeNameArray[currIndex])

		# maturityCutoff = parser.parse(maturityCutoffArray[index])
		maturityCutoff = currDate + timedelta(days=90)

		paramResult = runBondSpline(currDate, ccyName, bondTypeNameArray[currIndex], bondCurveNameArray[currIndex], maturityCutoff, paramEst,onTheRunExclArray[currIndex],offTheRunInclArray[currIndex])
		# paramEst = [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144] # Run the last one [0.1,0.1,0.1,0.1,10,10] [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144]

		tempSql = putParams(currDate, bondTypeNameArray[currIndex], paramResult)
		print currDate.strftime("%Y-%m-%d") + " " + ccyName + " DONE WITH " + str(onTheRunExclArray[currIndex]) + " ON THE RUNS EXCL AND " + str(offTheRunInclArray[currIndex]) + " OFF THE RUNS"

		prevDate = currDate
		currDate = fd.rolldate(currDate + timedelta(days=1),tempCal, "follow")

