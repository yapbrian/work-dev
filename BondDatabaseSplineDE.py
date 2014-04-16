import GovernmentBonds as gb
import FedTreasurySpline as fts
import RunBondSplineDE as rbsde
import findates as fd
import pandas as pd
import matplotlib.pyplot as plt
from datetime import *
from dateutil import parser
import sys

# General use arrays
fdCcyMap = {"USD":"us","CAD":"ca","GBP":"uk","EUR":"de"}
ccyNameArray = ["USD","CAD","GBP","EUR"]
bondTypeNameArray = ["USD_CASH_BOND_GOVT","CAD_CASH_BOND_GOVT","GBP_CASH_BOND_GOVT","DEM_CASH_BOND_GOVT"]
bondCurveNameArray = ["USD_CASH_BOND_GOVT_BMK","CAD_CASH_BOND_GOVT_BMK","GBP_CASH_BOND_GOVT_BMK","DEM_CASH_BOND_GOVT_BMK"]


def writeBondDatabaseSplineDE(asOfDate, ccyName, bondTypeName, bondCurveName):

	bondIDDict = getBondIDDictionary(bondTypeName)
	bondCurve = gb.GovernmentBondCurve(ccyName,bondCurveName)
	bondCurve.loadBondAllQuotes(asOfDate, bondTypeName)

	tempSeriesArray = []
	yieldArray = []
	predYieldArray = []
	cusipArray = []

	sqlValues = ""
	sqlDelValues = ""

	maturityCutoff = asOfDate + timedelta(days=90) #datetime(asOfDate.year, asOfDate.month+4, 1)

	# Aggregate the bond cashflow data
	for tempCUSIP, tempBondPricer in bondCurve.bondOffTheRunQuotes.items():
		tempBond = tempBondPricer.bondObj
		
		tempSeries = tempBond.generateCashflows()
		tempSeriesArray.append(tempSeries)
		yieldArray.append(tempBondPricer.calcTrueYieldPrice(tempBondPricer.price))
		cusipArray.append(tempCUSIP)

	[beta0,beta1,beta2,beta3,tau1,tau2] = rbsde.getStoredParams(asOfDate,bondTypeName)

	bondCal = fd.get_calendar(fdCcyMap[ccyName])
	settleDate = fd.rolldate(asOfDate + timedelta(days=tempBond.settleDelay), bondCal, "follow")

	fedSpline = fts.FedTreasurySpline(settleDate, tempSeriesArray)
	bondPricePred,macDurationPred = fedSpline.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

	# No need for calendar adjustment here
	rollDate = asOfDate + timedelta(days=90)
	settleRollDate = fd.rolldate(rollDate + timedelta(days=tempBond.settleDelay), bondCal, "follow")
	fedSplineRoll = fts.FedTreasurySpline(settleRollDate, tempSeriesArray)
	bondRollPricePred,macDurationPred = fedSplineRoll.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

	index = 0
	for tempCUSIP, tempBondPricer in bondCurve.bondOffTheRunQuotes.items():
		tempBond = tempBondPricer.bondObj
		# For calculatin roll yield
		if settleRollDate > tempBond.maturityDt:
			predRollYield = 0.0
		else:
			rolledBondPricer = gb.BondPricerClass(rollDate,tempBond)

			predYield = tempBondPricer.calcTrueYieldPrice(bondPricePred[index]-tempBondPricer.accrInt)
			predYieldArray.append(predYield)
			# print tempCUSIP + " " + str(bondRollPricePred[index])
			predRollYield = rolledBondPricer.calcTrueYieldPrice(bondRollPricePred[index]-rolledBondPricer.accrInt)

		rolldown = 10000*(predYield - predRollYield)
		actYield = yieldArray[index]
		diffYield = actYield - predYield

		if sqlValues == "":
			sqlValues = "({sqlAssetID},'{sqlSaveDate}',{sqlYield},{sqlPredYield},{sqlYieldDiff},{sqlRolldown})".format(sqlAssetID = bondIDDict[tempCUSIP],
					sqlSaveDate = asOfDate.strftime("%Y-%m-%d"),
					sqlPredYield = predYield,
					sqlYield = actYield,
					sqlYieldDiff = diffYield,
					sqlRolldown = rolldown)
			delSqlValues = str(bondIDDict[tempCUSIP])
		else:
			sqlValues = sqlValues + ",({sqlAssetID},'{sqlSaveDate}',{sqlYield},{sqlPredYield},{sqlYieldDiff},{sqlRolldown})".format(sqlAssetID = bondIDDict[tempCUSIP],
					sqlSaveDate = asOfDate.strftime("%Y-%m-%d"),
					sqlPredYield = predYield,
					sqlYield = actYield,
					sqlYieldDiff = diffYield,
					sqlRolldown = rolldown)
			delSqlValues = delSqlValues + "," + str(bondIDDict[tempCUSIP])
		
		index = index + 1

	if sqlValues != "":
		db = rbsde.msSQLConnect()
		cur = db.cursor()

		sqlStr = "DELETE FROM [SHA2].[rates_data].[bond_tsm_spline_quotes] WHERE bond_id IN ({sqlDelAssetID}) AND save_date = '{sqlSaveDate}'".format(sqlDelAssetID = delSqlValues, sqlSaveDate = asOfDate.strftime("%Y-%m-%d"))
		cur.execute(sqlStr)
		
		sqlStr = "INSERT INTO [SHA2].[rates_data].[bond_tsm_spline_quotes] (bond_id, save_date, yield_to_maturity, spline_yield_to_maturity, difference_spline_ytm, spline_rolldown_quote) VALUES {sqlInsertValues}".format(sqlInsertValues=sqlValues)
		cur.execute(sqlStr)

	return yieldArray, predYieldArray



# Returns a CUSIP Based hashtable of bonds within the database
def getBondIDDictionary(bondTypeName):
	bondDictionary = {}

	# SQL Connection
	db = rbsde.msSQLConnect()

	strSQL = """
		SELECT blt.cusip_id, blt.bond_id 
		FROM [SHA2].[rates_data].[bond_list_table] blt 
		LEFT JOIN [SHA2].[rates_data].[asset_table] at ON blt.asset_id = at.asset_id
		WHERE at.asset_name = '{sqlBondTypeName}'
		""".format(sqlBondTypeName = bondTypeName)
	
	cur = db.cursor()
	cur.execute(strSQL)

	for row in cur:
		bondDictionary[row.cusip_id] = row.bond_id

	return bondDictionary


def plotBondDatabaseSplineResults(asOfDate, ccyName, bondTypeName, bondCurveName):
	
	bondCurve = gb.GovernmentBondCurve(ccyName,bondCurveName)
	bondCurve.loadBondBmkQuotes(asOfDate)
	bondCurve.loadBondAllQuotes(asOfDate, bondTypeName)

	cusipToIndex = {}
	dataYieldArray = []
	dataSplineYieldArray = []

	onTheRunYieldArray = []
	onTheRunSplineYieldArray = []
	onTheRunMaturityArray = []

	offTheRunYieldArray = []
	offTheRunSplineYieldArray = []
	offTheRunMaturityArray = []

	db = rbsde.msSQLConnect()
	cur = db.cursor()

	# Get the quotes
	strSQL = """
	SELECT blt.cusip_id, btsq.save_date, btsq.yield_to_maturity, btsq.spline_yield_to_maturity, btsq.difference_spline_ytm, btsq.spline_rolldown_quote
	FROM [SHA2].[rates_data].[bond_tsm_spline_quotes] btsq 
	LEFT JOIN [SHA2].[rates_data].[bond_list_table] blt ON blt.bond_id = btsq.bond_id
	LEFT JOIN [SHA2].[rates_data].[asset_table] at ON blt.asset_id = at.asset_id
	WHERE btsq.save_date = '{sqlSaveDate}' AND at.asset_name = '{sqlBondTypeName}'
	""".format(
		sqlSaveDate = asOfDate.strftime("%Y-%m-%d"),
		sqlBondTypeName = bondTypeName)

	cur.execute(strSQL)
	index = 0
	for row in cur:
		cusipToIndex[row.cusip_id] = index
		dataYieldArray.append(row.yield_to_maturity)
		dataSplineYieldArray.append(row.spline_yield_to_maturity)
		index = index + 1

	for tempCUSIP, tempBondPricer in bondCurve.bondBmkCurveQuotes.items():
		tempBond = tempBondPricer.bondObj
		bondIndex = cusipToIndex[tempCUSIP]
		onTheRunMaturityArray.append(tempBond.maturityDt)
		onTheRunYieldArray.append(dataYieldArray[bondIndex])
		onTheRunSplineYieldArray.append(dataSplineYieldArray[bondIndex])

	for tempCUSIP, tempBondPricer in bondCurve.bondOffTheRunQuotes.items():
		tempBond = tempBondPricer.bondObj
		bondIndex = cusipToIndex[tempCUSIP]
		offTheRunMaturityArray.append(tempBond.maturityDt)
		offTheRunYieldArray.append(dataYieldArray[bondIndex])
		offTheRunSplineYieldArray.append(dataSplineYieldArray[bondIndex])

	plt.plot(offTheRunMaturityArray,offTheRunYieldArray,'r+', offTheRunMaturityArray,offTheRunSplineYieldArray,'g+', 
		onTheRunMaturityArray,onTheRunYieldArray,'ro', onTheRunMaturityArray,onTheRunSplineYieldArray,'go')

	plt.show()



####################################################################
################# Main processing module ###########################
####################################################################

if __name__ == "__main__":

	if len(sys.argv) < 2:
		print "Usage: ipython RatesBondSpline.py <DATE> <DATEEND> <CCY>"
		print "<DATE>,<DATEEND> is in YYYYMMDD format"
		print "<CCY> is a three letter currency code (supported: USD,CAD,GBP,EUR)"
		sys.exit(1)

	if len(sys.argv) == 2:
			
		asOfDate = parser.parse(sys.argv[1])

		for index,ccyName in enumerate(ccyNameArray):
			tempCal = fd.get_calendar(fdCcyMap[ccyName])

			writeBondDatabaseSplineDE(asOfDate,ccyName,bondTypeNameArray[index],bondCurveNameArray[index])

			print asOfDate.strftime("%Y-%m-%d") + " " + ccyName + " BOND DATA UPLOADED"


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

		while currDate < endDate:
			writeBondDatabaseSplineDE(currDate,ccyName,bondTypeNameArray[currIndex],bondCurveNameArray[currIndex])

			print currDate.strftime("%Y-%m-%d") + " " + ccyName + " BOND DATA UPLOADED"

			currDate = fd.rolldate(currDate + timedelta(days=1),tempCal, "follow")


