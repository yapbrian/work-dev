from dateutil import parser
import csv
import sys
import datetime
import pyodbc
import pandas.io.sql as psql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pdb #debugger

def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
	    db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
	    logging.warning("Database connection error. " + str(err))
	    sys.exit()
	return db

#### GET THE FX FOR A GIVEN PAIR ####
#### ONLY WORKS AS LONG AS EACH PAIR IS QUOTED VS. USD ####

def getFXData(startDate, endDate, numerator, denominator):
	
	if numerator != "USD" and denominator != "USD":
		qryString = """
				SELECT rdfl.asset_id, mmc1.currencySymbol as BaseFX, mmc2.currencySymbol as CounterFX FROM [SHA2].[rates_data].[fx_list] rdfl 
				LEFT JOIN [SHA2].[map].[map_currency] mmc1 ON mmc1.currencyID = rdfl.base_currency 
				LEFT JOIN [SHA2].[map].[map_currency] mmc2 ON mmc2.currencyID = rdfl.counter_currency
				WHERE mmc1.currencySymbol = '{sqlNum}' OR mmc1.currencySymbol = '{sqlDen}'
				OR mmc2.currencySymbol = '{sqlNum}' OR mmc2.currencySymbol = '{sqlDen}'
				""".format(
					sqlNum = numerator,
					sqlDen = denominator
					)
	else:
		if numerator == "USD":
			ccySymbol = denominator
		else:
			ccySymbol = numerator

		qryString = """
				SELECT rdfl.asset_id, mmc1.currencySymbol as BaseFX, mmc2.currencySymbol as CounterFX FROM [SHA2].[rates_data].[fx_list] rdfl 
				LEFT JOIN [SHA2].[map].[map_currency] mmc1 ON mmc1.currencyID = rdfl.base_currency
				LEFT JOIN [SHA2].[map].[map_currency] mmc2 ON mmc2.currencyID = rdfl.counter_Currency
				WHERE mmc1.currencySymbol = '{sqlCCYSymbol}' OR mmc2.currencySymbol = '{sqlCCYSymbol}'
				""".format(
					sqlCCYSymbol = ccySymbol
					)


	db = msSQLConnect()
	cur = db.cursor()
	cur.execute(qryString)
	strFXID = ""

	fxDictionary = {}
	# Everything is quoted vs. USD so construct the base against that
	for row in cur:
		# -ve in order to distinguish if on numerator or denominator (inverse)
		if row.BaseFX == "USD":
			fxDictionary[row.CounterFX] = -row.asset_id
		else:
			fxDictionary[row.BaseFX] = row.asset_id
		
		if strFXID == "":
			strFXID = str(row.asset_id)
		else:
			strFXID = strFXID + "," + str(row.asset_id)

	# Construct a dataframe query where the FXID 
	qryString = """
		SELECT rdfq.save_date, rdfq.asset_id, rdfq.rate_quote FROM [SHA2].[rates_data].[fx_quotes] rdfq 
		WHERE rdfq.asset_id IN ({sqlFXID})
		AND rdfq.save_date >= '{startDateSQL}' AND rdfq.save_date <= '{endDateSQL}'
		""".format( sqlFXID = str(strFXID),
					startDateSQL = startDate.strftime("%Y-%m-%d"),
					endDateSQL = endDate.strftime("%Y-%m-%d")
					)

	fxQuoteDF = psql.frame_query(qryString,db)
	fxQuoteDF["save_date"] = fxQuoteDF["save_date"].apply(parser.parse)
	fxQuoteDF = fxQuoteDF.pivot("save_date","asset_id","rate_quote")

	if len(fxDictionary) > 1:
		if fxDictionary[numerator] < 0:
			fxQuoteDF.ix[:,abs(fxDictionary[numerator])] = 1/fxQuoteDF.ix[:,abs(fxDictionary[numerator])]

		if fxDictionary[denominator] > 0:
			fxQuoteDF.ix[:,abs(fxDictionary[denominator])] = 1/fxQuoteDF.ix[:, abs(fxDictionary[denominator])]
	
		crossQuote = fxQuoteDF.iloc[:,0]*fxQuoteDF.iloc[:,1]
	else:
		crossQuote = fxQuoteDF.iloc[:,0]
		if numerator == "USD": 
			if fxDictionary[denominator] > 0:
				# then it is fine
				crossQuote = 1/fxQuoteDF.iloc[:,0]
		elif denominator == "USD":
			if fxDictionary[numerator] < 0:
				crossQuote = 1/fxQuoteDF.iloc[:,0]

	crossQuote = pd.DataFrame(crossQuote, crossQuote.index)
	crossQuote = crossQuote.dropna()
	return crossQuote


# return a pandas time series of the desired assetname
# asset name is a string name for the asset that is being searched for, swaps, futures, bonds, etc.
# startDate and endDate are datetime arguments that mark the start and end date of the timeseries
def getAssetData(startDate, endDate, assetName):
	# CHANGE TO SPLIT
	splitAssetType = assetName.split("_")
	splitAssetTypeSwap = splitAssetType[-3]
	splitAssetTypeFut = splitAssetType[-2]

	# Construct the time series query
	if splitAssetTypeFut.find("FUT") != -1:
		sqlString = """
			SELECT fq.save_date, fq.rate_quote FROM SHA2.rates_data.futures_quotes fq 
			LEFT JOIN SHA2.rates_data.asset_table at ON at.asset_id = fq.asset_id
			WHERE at.asset_name = '{assetNameSQL}' AND fq.save_date >= '{startDateSQL}' AND fq.save_date <= '{endDateSQL}'
			ORDER BY fq.save_date
			""".format(
				assetNameSQL = assetName,
				startDateSQL = startDate.strftime('%Y-%m-%d'),
				endDateSQL = endDate.strftime('%Y-%m-%d')
				)
	elif splitAssetTypeSwap.find("SWAP") != -1:
		sqlString = """
			SELECT sq.save_date, sq.rate_quote FROM SHA2.rates_data.swap_quotes sq 
			LEFT JOIN SHA2.rates_data.asset_table at ON at.asset_id = sq.asset_id
			WHERE at.asset_name = '{assetNameSQL}' AND sq.save_date >= '{startDateSQL}' AND sq.save_date <= '{endDateSQL}'
			ORDER BY sq.save_date
			""".format(
				assetNameSQL = assetName,
				startDateSQL = startDate.strftime('%Y-%m-%d'),
				endDateSQL = endDate.strftime('%Y-%m-%d')
				)
	elif splitAssetTypeSwap.find("BASIS") != -1:
		sqlString = """
			SELECT bq.save_date, bq.rate_quote FROM SHA2.rates_data.basis_quotes bq 
			LEFT JOIN SHA2.rates_data.asset_table at ON at.asset_id = bq.asset_id
			WHERE at.asset_name = '{assetNameSQL}' AND bq.save_date >= '{startDateSQL}' AND bq.save_date <= '{endDateSQL}'
			ORDER BY bq.save_date
			""".format(
				assetNameSQL = assetName,
				startDateSQL = startDate.strftime('%Y-%m-%d'),
				endDateSQL = endDate.strftime('%Y-%m-%d')
				)
	elif assetName.find("_FX") != -1:
		# For FX must assume it is given as GBP_USD_FX (which would be GBPUSD Curncy in BBG -> USD per 1 unit of GBP)
		# Break up the string - # note the lack of error checking
		fxArray = assetName.split("_")
		fxNum = fxArray[0]
		fxDen = fxArray[1]
		quoteSeries = getFXData(startDate, endDate, fxNum, fxDen)
		quoteSeries.columns = [assetName]
		return quoteSeries # No need to do the post proccessing below
	else:
		return None

	db = msSQLConnect()

	quoteSeries = psql.frame_query(sqlString,db)
	quoteSeries["save_date"] = quoteSeries["save_date"].apply(parser.parse)
	quoteSeries = quoteSeries.set_index("save_date")
	quoteSeries = quoteSeries.rename(columns={"rate_quote":assetName})

	return quoteSeries


def regressAssetData(startDate, endDate, yAssetName, xAssetNameList, onDeltas = False):
	yAssetSeries = getAssetData(startDate, endDate, yAssetName)

	assetSeriesArray = []

	assetSeriesArray.append(yAssetSeries)

	for assetName in xAssetNameList:
		xAssetSeries = getAssetData(startDate, endDate, assetName)
		assetSeriesArray.append(xAssetSeries)

	concatAssetSeries = pd.concat(assetSeriesArray, axis=1)
	concatAssetSeries = concatAssetSeries.dropna()

	if onDeltas == True:
		concatAssetSeries = concatAssetSeries.diff(periods=1)
		concatAssetSeries = concatAssetSeries.dropna()

	resultRegr=pd.ols(y=concatAssetSeries.iloc[:,0],x=concatAssetSeries.iloc[:,1:])
	return resultRegr

def plotRegressAssetData(startDate, endDate, predDate, yAssetName, xAssetNameList, onDeltas = False):

	regrData = regressAssetData(startDate, endDate, yAssetName, xAssetNameList, onDeltas)
	projectedSeries = pd.Series()
	outSampleDF = pd.DataFrame()
	actualSeries = getAssetData(endDate, predDate, yAssetName)
	# Run the projections
	for assetIndex,assetName in enumerate(xAssetNameList):
		# Returns the dataframe so the variable needs to be adjusted to Series
		xAssetSeries = getAssetData(endDate, predDate, assetName)
		xAssetSeries = regrData.beta.ix[assetIndex]*xAssetSeries.ix[:,0]
		projectedSeries = projectedSeries.add(xAssetSeries, fill_value=0)
		# Forget about intercept for now but can create a 1 series times intercept

	# intercept
	projectedSeries = projectedSeries + regrData.beta.ix[-1]
	# Turn into dataframe
	projectedSeries = pd.DataFrame(projectedSeries, projectedSeries.index)

	# Concatentate
	outSampleDF = pd.concat([actualSeries, projectedSeries], axis=1)
	outSampleDF.columns = ['Actual', 'Projected']
	outSampleDF = outSampleDF.dropna()

	if onDeltas == True:
		outSampleDF = outSampleDF.diff(periods=1)
		outSampleDF = outSampleDF.dropna()

	# Plot it proper
	fig, axes = plt.subplots(1,3, figsize=(15,5))
	# gradient
	colorMap = plt.get_cmap("Reds")
	graphColor = [ colorMap(float(i)/len(regrData.y)) for i in xrange(len(regrData.y)) ]
	axes[0].scatter(regrData.y_fitted, regrData.y, c=graphColor, marker='o')
	
	colorMap = plt.get_cmap("Greens")
	graphColor = [ colorMap(float(i)/len(outSampleDF.iloc[:,0])) for i in xrange(len(outSampleDF.iloc[:,0])) ]
	axes[0].scatter(outSampleDF.iloc[:,1], outSampleDF.iloc[:,0], c=graphColor, marker='o')

	strLabel = "Predicted: {0:.2f}  Actual: {1:.2f}  ({2})".format(outSampleDF.iloc[-1,1]*100,outSampleDF.iloc[-1,0]*100, outSampleDF.index[-1].strftime("%m/%d/%Y"))
	axes[0].scatter(outSampleDF.iloc[-1,1], outSampleDF.iloc[-1,0], c="Yellow", s=40, marker='s', label=strLabel)
	axes[0].set_title("Versus Fitted")
	axes[0].legend(loc='lower left', prop={'size':8})
	
	axes[1].plot(regrData.y.index, regrData.y, c="Red", label="Actual")
	graphLabel = "Fitted (R^2: {:.2f})".format(regrData.r2)
	axes[1].plot(regrData.y_fitted.index, regrData.y_fitted, c="Blue", label=graphLabel)

	# Out of sample
	axes[1].plot(outSampleDF.index, outSampleDF.iloc[:,0], c="Red")
	axes[1].plot(outSampleDF.index, outSampleDF.iloc[:,1], c="Green", label="Projection")

	axes[1].set_xticklabels(axes[1].xaxis.get_majorticklabels(), rotation=90)
	axes[1].xaxis_date()
	axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
	axes[1].set_title("Time Series")
	axes[1].legend(loc='lower left', prop={'size':8})
	
	colorMap = plt.get_cmap("spectral")
	colorCodes = [ colorMap(float(i)/len(xAssetNameList)) for i in xrange(len(xAssetNameList)) ]

	axes[2].plot(regrData.y.index, regrData.y, c="Red", label = "{0} (Int(bps):{1:.0f})".format(yAssetName,regrData.beta.ix[-1]*10000) )

	for xAssetNum in range(len(xAssetNameList)):
		graphLabel = "{assetName} (Beta: {betaVal:.2f})".format(assetName=xAssetNameList[xAssetNum], betaVal=regrData.beta.ix[xAssetNum])
		axes[2].plot(regrData.x.index, regrData.x.ix[:,xAssetNum], c=colorCodes[xAssetNum], label=graphLabel)
		axes[2].set_xticklabels(axes[2].xaxis.get_majorticklabels(), rotation=90)
		axes[2].xaxis_date()
		axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
		axes[2].set_title("Components")

	axes[2].legend(loc='lower left', prop={'size':8})
	return regrData



####################################################################
################# Main processing module ###########################
####################################################################

if len(sys.argv) != 5:
	print "Usage: ipython RatesDataBaseQueryFunctions.py <DATE> <SAMPLESIZE> <PREDSIZE> <PROCFILE>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

# curveName = sys.argv[1].upper()
predDate = parser.parse(sys.argv[1])
windowSize = int(sys.argv[2])
predSize = int(sys.argv[3])
inputFile = str(sys.argv[4]).strip()
endDate = predDate - datetime.timedelta(days=predSize)
startDate = endDate - datetime.timedelta(days=windowSize)

with open(inputFile,"rb") as f:
	reader = csv.reader(f, delimiter = ',')

	for row in reader:
		counter = 0
		xAssetNameList = []
		for assetName in row:
			if counter == 0:
				yAssetName = assetName.strip()
			else:
				xAssetNameList.append(assetName.strip())
			counter = counter + 1
			
		testR = plotRegressAssetData(startDate, endDate, predDate, yAssetName, xAssetNameList)

plt.show()

