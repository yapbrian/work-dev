from dateutil import parser
from math import copysign
import sys
import datetime
import pyodbc
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import pandas as pd

def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
	    db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
	    logging.warning("Database connection error. " + str(err))
	    sys.exit()
	return db


def calcBetaChanges(curveName, asOfDate, windowSize):
	db = msSQLConnect()

	numPeriods = windowSize*14/5

	earlyDate = asOfDate - datetime.timedelta(days=numPeriods)

	qryString = """
	SELECT fq.save_date, at.asset_id, fq.rate_quote FROM [SHA2].[rates_data].[futures_norm_quotes] fq 
		LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id 
		LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
		WHERE att.asset_type_name = '{sqlCurveName}' AND fq.save_date >= '{currDateOut}' AND fq.save_date <= '{asOfDateOut}'
		ORDER BY fq.save_date
		""".format(
			sqlCurveName = curveName,
			currDateOut = earlyDate.strftime('%Y-%m-%d'),
			asOfDateOut = asOfDate.strftime('%Y-%m-%d')
			)

	normFutDF = psql.frame_query(qryString,db)
	normFutDF["save_date"] = normFutDF["save_date"].apply(parser.parse)
	normFutDF = normFutDF.pivot("save_date","asset_id","rate_quote")

	# Compute the differences
	pxChgDF = normFutDF.diff(periods=1)
	pxChgDF = pxChgDF.dropna()
	# mean for beta calculation
	meanPXChgDF = pxChgDF.mean(axis=1)

	resultRegr = {}

	print "RESULTS for end date " + str(pxChgDF.index[-1].strftime('%Y-%m-%d'))

	plotSeriesAct = pd.Series()
	plotSeriesPred = pd.Series()

	countIndex = 0

	for nameIndex in pxChgDF.columns:
		resultRegr[nameIndex]=pd.ols(y=pxChgDF[nameIndex],x=meanPXChgDF, window=windowSize, window_type='rolling')
		expChange = resultRegr[nameIndex].beta.ix[-1,0] * meanPXChgDF[-1]
		upperBound = (resultRegr[nameIndex].beta.ix[-1,0] + copysign(1,meanPXChgDF[-1])*resultRegr[nameIndex].std_err.ix[-1,0])*meanPXChgDF[-1]
		lowerBound = (resultRegr[nameIndex].beta.ix[-1,0] - copysign(1,meanPXChgDF[-1])*resultRegr[nameIndex].std_err.ix[-1,0])*meanPXChgDF[-1]

		if pxChgDF[nameIndex][-1] > upperBound:
			outComment = "OUTPERFORMING"
		elif pxChgDF[nameIndex][-1] < lowerBound:
			outComment = "UNDERPERFORMING"
		else:
			outComment = "NEUTRAL"

		# print meanPXChgDF[-1]

		printStr = "Price: {pricePrint:.4f}\tPredicted: {expChgPrint:.4f} ({lowerBndPrint:.4f}, {upperBndPrint:.4f})\t Actual: {actChgPrint:.4f} ({rSquarePrint:.4f})\t[{cmtPrint}]".format(
				pricePrint = normFutDF[nameIndex][-1],
				expChgPrint = expChange,
				lowerBndPrint = lowerBound,
				upperBndPrint = upperBound,			
				actChgPrint = pxChgDF[nameIndex][-1],
				rSquarePrint = resultRegr[nameIndex].r2[-1],
				cmtPrint = outComment
				)
		print printStr

		# Compile two series
		plotSeriesAct = plotSeriesAct.append(pd.Series([pxChgDF[nameIndex][-1]],[countIndex]))
		plotSeriesPred = plotSeriesPred.append(pd.Series([expChange], [countIndex]))

		countIndex = countIndex+1

	print "COUNT: " + str(pxChgDF.columns.size)

	plotDataFrame = pd.DataFrame({"Actual":plotSeriesAct, "Predicted":plotSeriesPred})
	return plotDataFrame
	# fig, ax = plt.subplots()
	# ax.plot(plotSeriesAct.index, plotSeriesAct, label = "Actual " + curveName )
	# ax.plot(plotSeriesPred.index, plotSeriesPred, label = "Predicted " + curveName )

	# # plotSeriesAct.plot(label="Actual Px Change")
	# # plotSeriesPred.plot(label="Pred Px Change")
	# ax.legend()
	# # plt.show()



####################################################################
################# Main processing module ###########################
####################################################################

if len(sys.argv) != 3:
	print "Usage: ipython STIRNormFuturesBeta.py <DATE> <WINDOWSIZE>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

# curveName = sys.argv[1].upper()
asOfDate = parser.parse(sys.argv[1])
windowSize = int(sys.argv[2])

curveName = "USD_LIBOR_3M_FUT"
print "Curve: " + str(curveName) + "\tDate: " + str(asOfDate) + "\tWindow: " + str(windowSize)
dataFrameUSD = calcBetaChanges(curveName, asOfDate, windowSize)

curveName = "EUR_EURIBOR_3M_FUT"
print "Curve: " + str(curveName) + "\tDate: " + str(asOfDate) + "\tWindow: " + str(windowSize)
dataFrameEUR = calcBetaChanges(curveName, asOfDate, windowSize)

curveName = "CAD_CDOR_3M_FUT"
print "Curve: " + str(curveName) + "\tDate: " + str(asOfDate) + "\tWindow: " + str(windowSize)
dataFrameCAD = calcBetaChanges(curveName, asOfDate, windowSize)

fig, axes = plt.subplots(3,1, figsize=(8,10))
axes[0].plot(dataFrameUSD.index, dataFrameUSD)
axes[0].set_title("USD")
axes[1].plot(dataFrameEUR.index, dataFrameEUR)
axes[1].set_title("EUR")
axes[2].plot(dataFrameCAD.index, dataFrameCAD)
axes[2].set_title("CAD")

plt.show()
