import GovernmentBonds as gb
import FedTreasurySpline as fts
import matplotlib.pyplot as plt
import pandas as pd
import pyodbc
import findates as fd 	# Importa user defined library for financial date processing
from datetime import *
from scipy.optimize import leastsq
from dateutil import parser


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


fdCcyMap = {"USD":"us","CAD":"ca","GBP":"uk","EUR":"de"}
ccyName = "USD"
bondTypeName = "USD_CASH_BOND_GOVT"
bondCurveName = "USD_CASH_BOND_GOVT_BMK"

asOfDate = parser.parse("2009-02-19")

testClass = gb.GovernmentBondCurve(ccyName,bondCurveName)

testClass.loadBondBmkQuotes(asOfDate)
testClass.loadBondOffTheRunQuotes(asOfDate,1)

onTheRunPriceArray = []
onTheRunSeriesArray = []
onTheRunMaturityArray = []
onTheRunYieldArray = []
onTheRunPredYieldArray = []

onTheRunDictionary = {}
for tempCUSIP, tempBondPricer in testClass.bondBmkCurveQuotes.items():
	tempBond = tempBondPricer.bondObj
	tempSeries = tempBond.generateCashflows()
	onTheRunMaturityArray.append(tempBond.maturityDt)
	onTheRunPriceArray.append(tempBondPricer.price)
	onTheRunSeriesArray.append(tempSeries)
	onTheRunDictionary[tempCUSIP] = tempBond.maturityDt
	onTheRunYieldArray.append(tempBondPricer.calcTrueYieldPrice(tempBondPricer.price))

for tempCUSIP, tempBondPricer in testClass.bondOffTheRunQuotes.items():
	tempBond = tempBondPricer.bondObj
	onTheRunDictionary[tempCUSIP] = tempBond.maturityDt


# testClass.loadBondAllQuotes(asOfDate, bondTypeName)
testClass.loadBondOffTheRunQuotes(asOfDate,6)

tempSeriesArray = []
priceArray = []
maturityArray = []
yieldArray = []
predYieldArray = []
cusipArray = []

maturityCutoff = asOfDate + timedelta(days=90) #datetime(asOfDate.year, asOfDate.month+4, 1)

# Aggregate the bond cashflow data
for tempCUSIP, tempBondPricer in testClass.bondOffTheRunQuotes.items():
	tempBond = tempBondPricer.bondObj
	
	if tempBond.CUSIP not in onTheRunDictionary:
		if tempBond.maturityDt > maturityCutoff:
			tempSeries = tempBond.generateCashflows()
			tempSeriesArray.append(tempSeries)
			priceArray.append(tempBondPricer.price+tempBondPricer.accrInt)
			yieldArray.append(tempBondPricer.calcTrueYieldPrice(tempBondPricer.price))
			maturityArray.append(tempBond.maturityDt)
			cusipArray.append(tempCUSIP)
			# print str(tempBondPricer.calcPriceTrueYield(0.02)) + " " + tempBond.CUSIP

bondCal = fd.get_calendar(fdCcyMap[ccyName])
settleDate = fd.rolldate(asOfDate + timedelta(days=tempBond.settleDelay), bondCal, "follow")

fedSpline = fts.FedTreasurySpline(settleDate, tempSeriesArray)

#paramEst = [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144] # Run the last one [0.1,0.1,0.1,0.1,10,10] [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144]
#paramEst = [13.637,-13.625615191,-39.65844,19.34625,92.66853,66.80389]
#paramEst = [1.61425,0.766363,-2891.4374,-13.26685,21858,0.659582]
#paramEst = [0.5,0.5,0.5,0.5,10,10]

fitParams = getStoredParams(asOfDate,bondTypeName)

[beta0,beta1,beta2,beta3,tau1,tau2] = fitParams

endDate = datetime(settleDate.year+31,settleDate.month,15)
daysDiff = (endDate-settleDate).days
tempDates = [settleDate + timedelta(days=x) for x in range(0,daysDiff)]

bondPricePred,macDurationPred = fedSpline.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

fedSplineOTR = fts.FedTreasurySpline(settleDate, onTheRunSeriesArray)
bondOnTheRunPricePred, macDurationPred = fedSplineOTR.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

index = 0
for tempCUSIP, tempBondPricer in testClass.bondBmkCurveQuotes.items():
	tempBond = tempBondPricer.bondObj
	onTheRunPredYieldArray.append(tempBondPricer.calcTrueYieldPrice(bondOnTheRunPricePred[index]-tempBondPricer.accrInt))
	index = index + 1

index = 0
for tempCUSIP, tempBondPricer in testClass.bondOffTheRunQuotes.items():
	tempBond = tempBondPricer.bondObj
	if tempBond.CUSIP not in onTheRunDictionary:
		if tempBond.maturityDt > maturityCutoff:
			predYieldArray.append(tempBondPricer.calcTrueYieldPrice(bondPricePred[index]-tempBondPricer.accrInt))
			index = index + 1

zCurve = fedSpline.zeroCouponYields(beta0,beta1,beta2,beta3,tau1,tau2,tempDates)
pCurve = fedSpline.parYields(beta0,beta1,beta2,beta3,tau1,tau2,settleDate)
fCurve = fedSpline.forwardYields(beta0,beta1,beta2,beta3,tau1,tau2,settleDate)

plt.plot(zCurve.index, zCurve, 'r', pCurve.index, pCurve, 'g', fCurve.index, fCurve, 'b',maturityArray,yieldArray,'m+', maturityArray,predYieldArray,'c+', 
	onTheRunMaturityArray,onTheRunYieldArray,'mo', onTheRunMaturityArray,onTheRunPredYieldArray,'co')
plt.show()

