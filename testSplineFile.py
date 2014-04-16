import GovernmentBonds as gb
import FedTreasurySpline as fts
import matplotlib.pyplot as plt
import pandas as pd
import findates as fd 	# Importa user defined library for financial date processing
import RunBondSplineDE as rbsde
from datetime import *
from scipy.optimize import leastsq
from dateutil import parser

fdCcyMap = {"USD":"us","CAD":"ca","GBP":"uk","EUR":"de"}
ccyName = "USD"
bondTypeName = "USD_CASH_BOND_GOVT"
bondCurveName = "USD_CASH_BOND_GOVT_BMK"

asOfDate = parser.parse("2014-04-15")
rollDate = asOfDate + timedelta(days=90)

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


testClass.loadBondAllQuotes(asOfDate, bondTypeName)
#testClass.loadBondOffTheRunQuotes(asOfDate,6)

tempSeriesArray = []
priceArray = []
maturityArray = []
yieldArray = []
predYieldArray = []
cusipArray = []

maturityCutoff = asOfDate + timedelta(days=10) #datetime(asOfDate.year, asOfDate.month+4, 1)

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
settleRollDate = fd.rolldate(rollDate + timedelta(days=tempBond.settleDelay), bondCal, "follow")


#paramEst = [0.0365179,-0.03027,-0.0681372,0.01737,2.40274,4.88103249] # Run the last one [0.1,0.1,0.1,0.1,10,10] [-0.4144769, 0.409366, 36.7183353, 2.3119174458, -1827.1307, 78.787144]
#paramEst = [13.637,-13.625615191,-39.65844,19.34625,92.66853,66.80389]
#paramEst = [1.61425,0.766363,-2891.4374,-13.26685,21858,0.659582]
#paramEst = [0.5,0.5,0.5,0.5,10,10]
paramEst = rbsde.getStoredParams(asOfDate, bondTypeName)

# fitParams = leastsq(fedSpline.getResidual, paramEst, args=(priceArray), maxfev=10000)

#[beta0,beta1,beta2,beta3,tau1,tau2] = fitParams[0]
[beta0,beta1,beta2,beta3,tau1,tau2] = paramEst

endDate = datetime(settleDate.year+31,settleDate.month,15)
daysDiff = (endDate-settleDate).days
tempDates = [settleDate + timedelta(days=x) for x in range(0,daysDiff)]

fedSpline = fts.FedTreasurySpline(settleDate, tempSeriesArray)
bondPricePred,macDurationPred = fedSpline.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

fedSplineRoll = fts.FedTreasurySpline(settleRollDate, tempSeriesArray)
bondPricePredRoll,macDurationPred = fedSplineRoll.priceBonds(beta0,beta1,beta2,beta3,tau1,tau2)

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


# dateArray = []
# for i in range(35):
# 	dateArray.append(datetime(2016+i,1,1))
# 	dateArray.append(datetime(2016+i,6,1))

# testYlds,xDateArray,yYields = testClass.interpBondBmk(dateArray, True)

# plt.plot(dateArray,testYlds,"o", xDateArray,yYields, "-")

# plt.show()

# # Define the cubic spline and return the coefficients

# def cubicSplineCustom(xRange, yRange):
# 	xCount = len(xRange)
# 	yCount = len(yRange)

# 	if xCount != yCount:
# 		# Error handling
# 		print "Mismatched datapoints in cubic spline"
# 		return -999

# 	yt = [0.0 for x in range(xCount)]	# Second derivative values
# 	u = [0.0 for x in range(xCount)]

# 	for i in range(1, xCount - 2):
# 		sig = ( xRange[i] - xRange[i - 1] )/( xRange[i + 1] - xRange[i - 1] )
# 		p = sig * yRange[i - 1] + 2
# 		yt[i] = (sig - 1)/p
# 		u[i] = ( yRange[i+1] - yRange[i] ) / ( xRange[i+1] - xRange[i-1] ) - (yRange[i] - yRange[i-1])/(xRange[i] - xRange[i-1])
# 		u[i] = (6*u[i] / (xRange[i+1] - xRange[i-1]) - sig* u[i-1]) / p

# 	qn = 0
# 	un = 0

# 	yt[xCount-1] = (un - qn*u[xCount - 1]) / (qn*yt[xCount-1] + 1)

# 	for k in range(xCount-2, 0, -1):
# 		yt[k] = yt[k] * yt[k+1] + u[k]

# 	return zip(yRange,yt,u)


# def Splines(X,Y):
#     np1=len(X)
#     n=np1-1
#     X = [float(x) for x in X]
#     Y = [float(y) for y in Y]
#     a = Y[:]
#     b = [0.0]*(n)
#     d = [0.0]*(n)
#     h = [X[i+1]-X[i] for i in xrange(n)]
#     alpha = [0.0]*n
#     for i in xrange(1,n):
#         alpha[i] = 3/h[i]*(a[i+1]-a[i]) - 3/h[i-1]*(a[i]-a[i-1])
#     c = [0.0]*np1
#     L = [0.0]*np1
#     u = [0.0]*np1
#     z = [0.0]*np1
#     L[0] = 1.0; u[0] = z[0] = 0.0
#     for i in xrange(1,n):
#         L[i] = 2*(X[i+1]-X[i-1]) - h[i-1]*u[i-1]
#         u[i] = h[i]/L[i]
#         z[i] = (alpha[i]-h[i-1]*z[i-1])/L[i]
#     L[n] = 1.0; z[n] = c[n] = 0.0
#     for j in xrange(n-1, -1, -1):
#         c[j] = z[j] - u[j]*c[j+1]
#         b[j] = (a[j+1]-a[j])/h[j] - (h[j]*(c[j+1]+2*c[j]))/3
#         d[j] = (c[j+1]-c[j])/(3*h[j])
#     splines = []
#     for i in xrange(n):
#         splines.append((a[i],b[i],c[i],d[i],X[i]))
#     return splines,X[n]