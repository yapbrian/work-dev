from dateutil import parser
from datetime import *
from scipy import interpolate
import pyodbc
import math
import findates as fd
import pandas as pd
import numpy as np
import pdb # Debugger

class FedTreasurySpline(object):
	# Take as input an array of bondCashflow and turn them into a dataFrame, adjusted for accrued interest
	def __init__(self, settleDate, bondCashflowSeriesArray):
		# easiest to create a settleDate with a dummy dataFrame and add it on to the cashflows
		tempSettleDF = pd.DataFrame([999],index=[settleDate],columns=['DUMMY'])
		# Need to calc accrued interest or alternatively just do it with accrued interest
		self.dfBondCashflow = pd.concat(bondCashflowSeriesArray, axis=1)
		tempColOrder = self.dfBondCashflow.columns.tolist()

		self.dfBondCashflow = self.dfBondCashflow.append(tempSettleDF)	# This will add a row if it's not there
		# For some reason appending changes the order so re-order it back
		del self.dfBondCashflow['DUMMY']
		# Now sort since settledate is in there
		self.dfBondCashflow = self.dfBondCashflow[self.dfBondCashflow.index >= settleDate]
		self.dfBondCashflow = self.dfBondCashflow.sort_index(axis=0,ascending=True)
		self.dfBondCashflow = self.dfBondCashflow[tempColOrder]

		# Create the time deltas using the fd
		tempDates = self.dfBondCashflow.index.tolist()
		tempYearFrac = fd.yearfractions(tempDates,"ACT/ACT")	# Always actual/actual since we are discounting real value

		self.dfYearFrac = pd.DataFrame(tempYearFrac, index=tempDates, columns=['YEARFRAC'])

		# so you can get the list by CUSIPs (for pricing)
		# self.dfBondCashflow.tolist()


	# This uses the zero coupon yield formula as defined in the NY Fed paper by Gurkaynak, Sack and Wright (2006)
	# The yield curve is generated given a set of parameters B0, B1, B2, B3, tau1, tau2
	# The price is calculated (dirty) and returned as an array
	def priceBonds(self, beta0, beta1, beta2, beta3, tau1, tau2):
		priceArray = []
		macDurationArray = []
		# Compute some intermediate calculations
		nOverTau1 = self.dfYearFrac / tau1
		nOverTau2 = self.dfYearFrac / tau2
		expNOverTau1 = -nOverTau1
		expNOverTau2 = -nOverTau2
		expNOverTau1 = pd.DataFrame(expNOverTau1['YEARFRAC'].apply(math.exp), expNOverTau1.index, columns=['YEARFRAC'])
		expNOverTau2 = pd.DataFrame(expNOverTau2['YEARFRAC'].apply(math.exp), expNOverTau2.index, columns=['YEARFRAC'])

		zeroCouponValues = beta0 + beta1*(1-expNOverTau1)/(nOverTau1) + beta2*((1-expNOverTau1)/(nOverTau1) - expNOverTau1) + beta3*((1-expNOverTau2)/(nOverTau2)-expNOverTau2)
		zeroCurveValues = zeroCouponValues * self.dfYearFrac * (-1)		
		zeroCurveValues = pd.DataFrame(zeroCurveValues['YEARFRAC'].apply(math.exp), zeroCurveValues.index, columns = ['DISCOUNTRATE'])
		#self.zcVal = zeroCouponValues # DEBUG CODE
		#self.zCurVal = zeroCurveValues # DEBUG CODE

		for index, cpnSeries in self.dfBondCashflow.iteritems():
			tmpPrice = zeroCurveValues * cpnSeries
			tmpMacDuration = tmpPrice * self.dfYearFrac['YEARFRAC']	# Dataframe column times dataframe column results in Series?
			tmpPrice = tmpPrice.sum(axis=0).values[0]	# Convert to just a number
			tmpMacDuration = tmpMacDuration.sum(axis=0).values[0] / tmpPrice
			priceArray.append(tmpPrice)
			macDurationArray.append(tmpMacDuration)

		# Return the priceArray
		return priceArray, macDurationArray

	def getResidual(self, curveParams, priceMeas):
		[beta0, beta1, beta2, beta3, tau1, tau2] = curveParams
		# According to fed model, minimize the duration weighted sum of squares difference (approximately minimizing yields)
		tmpPrice, tmpMacDuration = self.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)
		npBondPrice = np.float64(tmpPrice)
		npMacDuration = np.float64(tmpMacDuration)
		npPriceMeas = np.float64(priceMeas)
		return (npPriceMeas - npBondPrice) / npMacDuration

	# Given a dateArray (array of datetime), return a series with an evaluated spline function
	def zeroCouponYields(self, beta0, beta1, beta2, beta3, tau1, tau2, dateArray):
		tempYearFrac = fd.yearfractions(dateArray,"ACT/ACT")
		dfZCValues = pd.DataFrame(tempYearFrac, index = dateArray, columns = ['YEARFRAC'])

		nOverTau1 = dfZCValues / tau1
		nOverTau2 = dfZCValues / tau2
		expNOverTau1 = -nOverTau1
		expNOverTau2 = -nOverTau2
		expNOverTau1 = pd.DataFrame(expNOverTau1['YEARFRAC'].apply(math.exp), expNOverTau1.index, columns=['YEARFRAC'])
		expNOverTau2 = pd.DataFrame(expNOverTau2['YEARFRAC'].apply(math.exp), expNOverTau2.index, columns=['YEARFRAC'])

		dfZCValues = beta0 + beta1*(1-expNOverTau1)/(nOverTau1) + beta2*((1-expNOverTau1)/(nOverTau1) - expNOverTau1) + beta3*((1-expNOverTau2)/(nOverTau2)-expNOverTau2)

		return dfZCValues

	# Return a par yield curve out to 30 years split semi-annually
	def parYields(self, beta0, beta1, beta2, beta3, tau1, tau2, startDate):
		# Compute as if continuous but calculate with daily
		endDate = datetime(startDate.year+30,startDate.month,startDate.day)
		daysDiff = (endDate-startDate).days
		tempDates = [startDate + timedelta(days=x) for x in range(0,daysDiff)]

		tempYearFrac = fd.yearfractions(tempDates,"ACT/ACT")
		tempYearFrac = pd.DataFrame(tempYearFrac, index=tempDates, columns=['YEARFRAC'])
		tempYearDeltas = tempYearFrac.diff(periods=1)

		tempZCValues = self.zeroCouponYields(beta0,beta1,beta2,beta3,tau1,tau2,tempDates)
		
		# pdb.set_trace()

		tempZCDiscount = tempZCValues * tempYearFrac * (-1)		
		tempZCDiscount = pd.DataFrame(tempZCDiscount['YEARFRAC'].apply(math.exp), tempZCDiscount.index, columns = ['DISCOUNTRATE'])

		tempDenom = tempZCDiscount*tempYearDeltas['YEARFRAC']
		tempDenom = tempDenom.cumsum(axis=0)
		
		tempNum = 1.0 - tempZCDiscount
		
		parYields = tempNum / tempDenom
		return parYields

	def forwardYields(self, beta0, beta1, beta2, beta3, tau1, tau2, startDate):
		endDate = datetime(startDate.year+30,startDate.month,startDate.day)
		daysDiff = (endDate-startDate).days
		tempDates = [startDate + timedelta(days=x) for x in range(0,daysDiff)]

		tempYearFrac = fd.yearfractions(tempDates,"ACT/ACT")
		tempYearFrac = pd.DataFrame(tempYearFrac, index=tempDates, columns=['YEARFRAC'])

		nOverTau1 = tempYearFrac / tau1
		nOverTau2 = tempYearFrac / tau2
		expNOverTau1 = -nOverTau1
		expNOverTau2 = -nOverTau2
		expNOverTau1 = pd.DataFrame(expNOverTau1['YEARFRAC'].apply(math.exp), expNOverTau1.index, columns=['YEARFRAC'])
		expNOverTau2 = pd.DataFrame(expNOverTau2['YEARFRAC'].apply(math.exp), expNOverTau2.index, columns=['YEARFRAC'])

		dfFwdCurveValues = beta0 + beta1*expNOverTau1 + beta2*nOverTau1*expNOverTau1 + beta3*nOverTau2*expNOverTau2

		return dfFwdCurveValues








