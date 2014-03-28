from datetime import *
from dateutil import parser
from scipy.optimize import leastsq
import pyodbc
import findates as fd
import pandas as pd
import calendar
import math
import pdb

freqDictionary = {"SemiAnnual":2, "Annual":1, "Quarterly":4, "Monthly":12}
# Currency map for calendar in finDates module
fdCcyMap = {"USD":"us","CAD":"ca","GBP":"uk","EUR":"de"}
fdBusDayConvMap = {"Following":"follow","Modified":"modfollow","Previous":"previous","ModifiedPrev":"modprevious"}
fdDayCountMap = {"ActualActualBond":"ACTUAL/ACTUAL"}


# Pricer object - stores pricing date and other price relevant details of bond
class BondPricerClass(object):
	def __init__(self, asOfDate, bondClassObj):

		self.asOfDate = asOfDate		
		# just to store pricing
		self.price = 100
		# just to store yield (#Street convention, no calculation yet)
		self.bondYld = 0.02
		# accrInt
		self.accrInt = 0

		self.bondObj = bondClassObj

		tempBondCal = fd.get_calendar(fdCcyMap[self.bondObj.ccyCode])
		self.settleDate = fd.rolldate(asOfDate + timedelta(days=self.bondObj.settleDelay), tempBondCal, fdBusDayConvMap[self.bondObj.busDayConv])

		# bondPrice, bondAccInt, bondYield, 
		tempPmtSeries = self.bondObj.paySchedule
		# Get a time series with settleDate
		try:
			tempPmtSeries[self.settleDate] = 0
		except KeyError:
			tempPmtSeries = tempPmtSeries.append(pd.Series([0],[self.settleDate]))

		tempPmtSeries = tempPmtSeries.sort_index(ascending=True)

		# calc accrued interest (use first pmt date before settleDate)
		try:
			tempAccrDate = tempPmtSeries[tempPmtSeries.index < self.settleDate].index[-1]
			self.accrInt = self.bondObj.coupon / freqDictionary[self.bondObj.payFreq] * fd.yearfrac(self.settleDate, tempAccrDate, fdDayCountMap[self.bondObj.dayCount])
		except IndexError:
			self.accrInt = 0

		tempPmtSeries = tempPmtSeries[tempPmtSeries.index >= self.settleDate]

		self.payScheduleSettle = tempPmtSeries

	# Calculate true yield (based on cashflows)
	def calcTrueYieldPrice(self, cleanPriceToFit, cmpFreq=2):
		# declare lambda function to fit
		# Fit's the cleanprice
		yieldFunc = lambda yieldToTry, priceToFit: priceToFit - self.calcPriceTrueYield(yieldToTry, cmpFreq)

		yieldCalc = leastsq(yieldFunc, 0.02, args=[cleanPriceToFit])
		return yieldCalc[0][0]


	# Calculate true clean price (based on cashflows and a given yield - adjusted for proper payments on cashflows, given compounding frequency (default semi-annual))
	# May need a pricer function since daycounts etc. are almost currency dependent
	def calcPriceTrueYield(self, trueYield, cmpFreq = 2):
		ctsTrueYield = cmpFreq * math.log(1.0+trueYield/cmpFreq)

		tempPmtSeries = self.payScheduleSettle

		tempDates = tempPmtSeries.index.tolist()
		tempYearFrac = fd.yearfractions(tempDates,"BOND BASIS")
		tempYearFrac = pd.Series(tempYearFrac, index=tempPmtSeries.index)

		tempExp1 = (-1)*tempYearFrac * ctsTrueYield
		tempExp1 = tempExp1.apply(math.exp)
		priceTrueYield = (tempPmtSeries*tempExp1).sum(axis=0)

		return (priceTrueYield - self.accrInt)


# Descriptive bond object - only data storage for now
class BondClass(object):
	def __init__(self, ccyCode, bondName, bondCUSIP, bondCoupon, bondIssueDt, bondMaturityDt, bondPayFreq, bondDayCount, bondBusDayConv, settleDelay):
		# Currency code for calendar and curve identification
		self.ccyCode = ccyCode.upper()
		# Descriptive stuff
		self.name = bondName.upper()
		# Descriptive stuff
		self.CUSIP = bondCUSIP.upper()

		self.coupon = bondCoupon

		self.issueDt = parser.parse(bondIssueDt)

		self.maturityDt = parser.parse(bondMaturityDt)
		# Frequency of coupon payments
		self.payFreq = bondPayFreq

		# Daycount for calculating payment of coupons
		self.dayCount = bondDayCount

		# Bus day convention for adjusting
		self.busDayConv = bondBusDayConv

		# Settlement delay
		self.settleDelay = settleDelay
		# Can run pricing functions within this class to reprice but atm just take as given
		# Would also want proper queries to internal objects using dereferences
		self.paySchedule = self.generateCashflows()


	# Generate an array of cashflows represented by the bond
	def generateCashflows(self):

		pmtDates = []
		pmtAmounts = []
		pmtCalendar = fd.get_calendar(fdCcyMap[self.ccyCode])

		# Calc the coupon from maturity date backwards
		currDate = self.maturityDt
		payDay = self.maturityDt.day

		couponPmt = self.coupon * 100.0 / freqDictionary[self.payFreq]
		pmtMonth = 12/freqDictionary[self.payFreq]

		# tmpMtLastDate = fd.lbusdate(currDate.year, currDate.month, pmtCalendar)		
		lastBusDay = True if payDay == calendar.monthrange(self.maturityDt.year, self.maturityDt.month)[1] else False

		# produce the dates - no payment on first coupon date (there for accrual calc)
		while(currDate > self.issueDt):
			# Determine whether it is middle of month payment date

			prevMonth = currDate.month - pmtMonth
			prevYear = currDate.year - 1 if prevMonth <= 0 else currDate.year
			prevMonth = (prevMonth % 12) if prevMonth <= 0 else prevMonth
			prevMonth = 12 if prevMonth == 0 else prevMonth
			
			prevDate = fd.rolldate(datetime(prevYear,prevMonth,payDay),pmtCalendar,fdBusDayConvMap[self.busDayConv]) if not lastBusDay else fd.lbusdate(prevYear, prevMonth, pmtCalendar)
			pmtDates.append(currDate)
			pmtAmounts.append(couponPmt) # Bonds only pay coupon unadjusted. -> daycount adj = couponPmt * freqDictionary[self.payFreq] *fd.yearfrac(prevDate,currDate,fdDayCountMap[self.dayCount]))	# Replace 365 with actual
			currDate = prevDate

		# include the issue date for calculation of accruals
		pmtDates.append(self.issueDt)
		pmtAmounts.append(0)

		pmtAmounts[0] = pmtAmounts[0] + 100.0

		pmtDates = [pDate for pDate in reversed(pmtDates)]
		pmtAmounts = [pAmts for pAmts in reversed(pmtAmounts)]

		tempSeries = pd.Series(pmtAmounts, pmtDates, name=self.CUSIP)

		self.paySchedule = tempSeries
		# Return both array
		return self.paySchedule


class GovernmentBondCurve(object):
	def __init__(self, ccyCode, curveName):
		self.curveName = curveName.upper()
		self.ccyCode = ccyCode.upper()
		self.saveDate = date.today()
		self.bondBmkCurveQuotes = {}
		self.bondOffTheRunQuotes = {}

	def loadBondBmkQuotes(self, saveDate):
		db = self.msSQLConnect()
		cur = db.cursor()
		self.saveDate = saveDate
		self.bondBmkCurveQuotes = {}
		# self.saveDate = parser.parse(saveDate)
		strSql = """
		SELECT bq.save_date, bq.bond_id, blt.cusip_id, blt.bond_name, bpt.bond_issue_date, bpt.bond_mat_date, bpt.currency_code, bpt.bond_coupon, bq.clean_price, bq.accrued_int, bq.asset_swap_level, bq.yield_to_maturity, bq.zspread_level,
			bpt.daycount_code, bpt.bond_pmt_frequency, bpt.busdayconv_code, bts.settle_delay
			FROM [SHA2].[rates_data].[bond_quotes] bq
			LEFT JOIN [SHA2].[rates_data].[bond_list_table] blt ON blt.bond_id = bq.bond_id 
			LEFT JOIN [SHA2].[rates_data].[bond_param_table] bpt ON bpt.bond_id = bq.bond_id 
			LEFT JOIN [SHA2].[rates_data].[bond_benchmark_map] bbm ON bq.bond_id = bbm.bond_id
			LEFT JOIN [SHA2].[rates_data].[bond_benchmark] bb ON bb.bond_benchmark_id = bbm.bond_benchmark_id 
			LEFT JOIN [SHA2].[rates_data].[bond_benchmark_curve] bbc ON bbc.bond_benchmark_curve_id = bb.bond_benchmark_curve_id 
			LEFT JOIN [SHA2].[rates_data].[bond_type_settings] bts ON bpt.bond_type_settings_id = bts.bond_type_settings_id
			INNER JOIN 
			(SELECT max(temp_bbm.save_date) AS max_save_date, temp_bbm.bond_benchmark_id AS max_bond_b_id 
			FROM [SHA2].[rates_data].[bond_benchmark_map] temp_bbm WHERE temp_bbm.save_date <= '{sqlSaveDate}' 
			GROUP BY temp_bbm.bond_benchmark_id) 
			AS temp_qry ON temp_qry.max_save_date = bbm.save_date 
			AND temp_qry.max_bond_b_id = bbm.bond_benchmark_id
			WHERE bbc.bond_benchmark_curve_name = '{sqlCurveName}' AND bq.save_date = '{sqlSaveDate}'
			ORDER BY bpt.bond_mat_date
			""".format(
				sqlSaveDate = self.saveDate.strftime('%Y-%m-%d'),
				sqlCurveName = self.curveName
				)
		cur.execute(strSql)
        
		for row in cur:
			tempBond = BondClass(row.currency_code, row.bond_name, row.cusip_id, row.bond_coupon, row.bond_issue_date, row.bond_mat_date, row.bond_pmt_frequency, row.daycount_code, row.busdayconv_code, row.settle_delay)
			tempBondPricer = BondPricerClass(self.saveDate, tempBond)
			tempBondPricer.price = row.clean_price
			tempBondPricer.accrInt = row.accrued_int
			tempBondPricer.bondYld = row.yield_to_maturity
			# row.clean_price, row.accrued_int, row.yield_to_maturity, 
			self.bondBmkCurveQuotes[tempBond.CUSIP] = tempBondPricer

		return self.bondBmkCurveQuotes

	def loadBondOffTheRunQuotes(self, saveDate, numBonds = 4):
		# Load the off the run quotes to numBonds last off the runs
		numBonds = numBonds + 1 # since one bond is always on the run actual numBonds has to be +1 of what is requested
		db = self.msSQLConnect()
		cur = db.cursor()
		self.saveDate = saveDate
		self.bondOffTheRunQuotes={}
		# self.saveDate = parser.parse(saveDate)

		strSql = """
		SELECT bb.bond_benchmark_id FROM [SHA2].[rates_data].[bond_benchmark] bb
			LEFT JOIN [SHA2].[rates_data].[bond_benchmark_curve] bbc ON bb.bond_benchmark_curve_id = bbc.bond_benchmark_curve_id
			WHERE bbc.bond_benchmark_curve_name = '{0}'
			""".format(self.curveName)

		cur.execute(strSql)

		strSql = ""
		queryNum = 0
		for row in cur:
			tempStrSql = """
			SELECT TOP {sqlNumBonds} bq.bond_id, blt.cusip_id, blt.bond_name, bpt.bond_issue_date, bpt.bond_mat_date, bpt.currency_code, bpt.bond_coupon, bq.clean_price, bq.accrued_int, bq.asset_swap_level, bq.yield_to_maturity, bq.zspread_level,
				bpt.daycount_code, bpt.bond_pmt_frequency, bpt.busdayconv_code, bts.settle_delay
				FROM [SHA2].[rates_data].[bond_quotes] bq
				LEFT JOIN [SHA2].[rates_data].[bond_list_table] blt ON blt.bond_id = bq.bond_id 
				LEFT JOIN [SHA2].[rates_data].[bond_param_table] bpt ON bpt.bond_id = bq.bond_id 
				LEFT JOIN [SHA2].[rates_data].[bond_benchmark_map] bbm ON bq.bond_id = bbm.bond_id
				LEFT JOIN [SHA2].[rates_data].[bond_benchmark] bb ON bb.bond_benchmark_id = bbm.bond_benchmark_id
				LEFT JOIN [SHA2].[rates_data].[bond_type_settings] bts ON bpt.bond_type_settings_id = bts.bond_type_settings_id
				WHERE bq.save_date = '{sqlSaveDate}' AND bbm.save_date <= '{sqlSaveDate}' 
				AND bbm.bond_benchmark_id = {sqlBondID}
				ORDER BY bbm.save_date DESC
				""".format(
					sqlNumBonds = numBonds,
					sqlSaveDate = self.saveDate.strftime("%Y-%m-%d"),
					sqlBondID = row.bond_benchmark_id
					)

			if strSql == "":
				strSql = "SELECT * FROM ({sqlTempStrSql}) query{sqlQueryNum}".format(sqlTempStrSql=tempStrSql,sqlQueryNum=queryNum)
			else:
				strSql = strSql + " UNION SELECT * FROM ({sqlTempStrSql}) query{sqlQueryNum}".format(sqlTempStrSql=tempStrSql,sqlQueryNum=queryNum)

			queryNum = queryNum + 1

		strSql = strSql + " ORDER BY bond_mat_date"

		cur.execute(strSql)

		# Add to the array any bond that is not in the on the run
		for row in cur:
			tempBond = BondClass(row.currency_code, row.bond_name, row.cusip_id, row.bond_coupon, row.bond_issue_date, row.bond_mat_date, row.bond_pmt_frequency, row.daycount_code, row.busdayconv_code, row.settle_delay)
			# row.clean_price, row.accrued_int, row.yield_to_maturity, 
			tempBondPricer = BondPricerClass(self.saveDate, tempBond)
			tempBondPricer.price = row.clean_price
			tempBondPricer.accrInt = row.accrued_int
			tempBondPricer.bondYld = row.yield_to_maturity
			# row.clean_price, row.accrued_int, row.yield_to_maturity, 

			if tempBond.CUSIP not in self.bondBmkCurveQuotes:
				self.bondOffTheRunQuotes[tempBond.CUSIP] = tempBondPricer

		return self.bondOffTheRunQuotes

	def loadBondAllQuotes(self, saveDate, bondAssetName):
		db = self.msSQLConnect()
		cur = db.cursor()
		self.saveDate = saveDate
		self.bondOffTheRunQuotes={}
		# self.saveDate = parser.parse(saveDate)
	
		strSql = """
		SELECT bq.bond_id, blt.cusip_id, blt.bond_name, bpt.bond_issue_date, bpt.bond_mat_date, bpt.currency_code, bpt.bond_coupon, bq.clean_price, bq.accrued_int, bq.asset_swap_level, bq.yield_to_maturity, bq.zspread_level,
			bpt.daycount_code, bpt.bond_pmt_frequency, bpt.busdayconv_code, bts.settle_delay
			FROM [SHA2].[rates_data].[bond_quotes] bq
			LEFT JOIN [SHA2].[rates_data].[bond_list_table] blt ON blt.bond_id = bq.bond_id 
			LEFT JOIN [SHA2].[rates_data].[asset_table] at ON at.asset_id = blt.asset_id
			LEFT JOIN [SHA2].[rates_data].[bond_param_table] bpt ON bpt.bond_id = bq.bond_id 
			LEFT JOIN [SHA2].[rates_data].[bond_type_settings] bts ON bpt.bond_type_settings_id = bts.bond_type_settings_id
			WHERE bq.save_date = '{sqlSaveDate}' AND at.asset_name = '{sqlAssetName}'
			ORDER BY bpt.bond_mat_date
			""".format(
				sqlSaveDate = self.saveDate.strftime("%Y-%m-%d"),
				sqlAssetName = bondAssetName
				)

		cur.execute(strSql)

		# Add to the array any bond that is not in the on the run
		for row in cur:
			tempBond = BondClass(row.currency_code, row.bond_name, row.cusip_id, row.bond_coupon, row.bond_issue_date, row.bond_mat_date, row.bond_pmt_frequency, row.daycount_code, row.busdayconv_code, row.settle_delay)
			# row.clean_price, row.accrued_int, row.yield_to_maturity, 
			tempBondPricer = BondPricerClass(self.saveDate, tempBond)
			tempBondPricer.price = row.clean_price
			tempBondPricer.accrInt = row.accrued_int
			tempBondPricer.bondYld = row.yield_to_maturity
			# row.clean_price, row.accrued_int, row.yield_to_maturity, 

			if tempBond.CUSIP not in self.bondBmkCurveQuotes:
				self.bondOffTheRunQuotes[tempBond.CUSIP] = tempBondPricer

		return self.bondOffTheRunQuotes


	def interpBondBmk(self, dateArray, offTheRunFlag = False):
		# assuming bondBmkCurveQuotes is loaded
		# create a y value of yields, x value of days from saveDate to maturity of corresponding yields
		# spline the dateArray
		xDays = []
		xDateArray = []
		yYields = []
		interpDays = []
		# Weights, weight the BMKs higher
		splineWgts = []

		# bmkCount = len(self.bondBmkCurveQuotes)
		# otrCount = len(self.bondOffTheRunQuotes)

		for iCUSIP,tempBond in self.bondBmkCurveQuotes.items():
			xDateArray.append(tempBond.maturityDt)
			yYields.append(tempBond.bondYld)
			splineWgts.append(10)

		if offTheRunFlag:
			for iCUSIP,tempBond in self.bondOffTheRunQuotes.items():
				xDateArray.append(tempBond.maturityDt)
				yYields.append(tempBond.bondYld)
				splineWgts.append(4)


		xDateArray, yYields = (list(x) for x in zip(*sorted(zip(xDateArray, yYields))))

		for tempDate in xDateArray:
			xDays.append((tempDate-self.saveDate).days)

		for tempDate in dateArray:
			interpDays.append((tempDate-self.saveDate).days)

		# Fill outside with 30 year yield
		# interpYld = interpolate.interp1d(xDays, yYields, kind='cubic', bounds_error=False, fill_value=yYields[-1])
		interpYld = interpolate.UnivariateSpline(xDays,yYields,w=splineWgts)
		# interpTCK = interpolate.splrep(xDays, yYields)
		# interpYld = interpolate.splev(interpDays, interpTCK)

		return interpYld(interpDays),xDateArray,yYields


	def msSQLConnect(self):
		dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
		try:
			db = pyodbc.connect(dbConnectionStr, autocommit = True)
		except pyodbc.Error, err:
			logging.warning("Database connection error. " + str(err))
			sys.exit()
		return db











