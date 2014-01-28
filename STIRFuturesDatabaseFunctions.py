from dateutil import parser
from datetime import *
import calendar
import pyodbc
import sys
from ShortRateFutures import *

# For each currency map to the appropriate curve
ccyToFutMap = {'USD':'USD_LIBOR_3M_FUT', 'CAD':'CAD_CDOR_3M_FUT', 'EUR':'EUR_EURIBOR_3M_FUT'}  # 'GBP':'GBP_LIBOR_3M_FUT', - left out until calendar corrected
ccyToMMktMap = {'USD':'USD_LIBOR_3M_MM_3M', 'CAD':'CAD_CDOR_3M_MM_3M', 'EUR':'EUR_EURIBOR_3M_MM_3M'}  # 'GBP':'GBP_LIBOR_3M_MM_3M',

def addMonths(sourceDate,months):
	month = sourceDate.month - 1 + months
	year = sourceDate.year + month / 12
	month = month % 12 + 1
	day = min(sourceDate.day,calendar.monthrange(year,month)[1])
	return datetime(year,month,day)

def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
	    db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
	    logging.warning("Database connection error. " + str(err))
	    sys.exit()
	return db

# Calculate the constant maturity levels for the futures
def calcConstantMaturityPrices(priceDate):
	valAddStr = ""

	# for each future in ccyToFutMap, grab all the futures in order
	for ccy, curveName in ccyToFutMap.items():
		adjQuoteArray = []
		tempFutCurve = ShortRateFuturesCurve(curveName)
		tempFutCurve.loadCurveQuotes(priceDate)

		prevQuote = 0
		prevMat = 0

		# Get the money market quote
		tempDate = parser.parse(priceDate)	# tempDate to use date functions
		mmktQuoteName = ccyToMMktMap[ccy]
		db = msSQLConnect()
		cur = db.cursor()
		# Aggregate positions for ease of use - DO NOT HAVE MATURITIES OF UNDERLYING YET IN MX
		sqlString = """
			SELECT mmq.save_date, mmq.eff_date, mmq.rate_quote, mmq.asset_id 
				FROM [SHA2].[rates_data].[moneymarkets_quotes] mmq 
				LEFT JOIN [SHA2].[rates_data].[asset_table] at ON mmq.asset_id = at.asset_id
				WHERE at.asset_name = '{mmktAssetName}' 
				AND mmq.save_date = '{sqlDate}'
			""".format(
				mmktAssetName = mmktQuoteName,
				sqlDate = tempDate.strftime('%Y-%m-%d')
				)
		cur.execute(sqlString)

		# interp from date using the MMkt quote as the first spot rate
		for row in cur:
			prevQuote = 100-row.rate_quote*100
			prevMat = parser.parse(row.eff_date)	# should be spot

		index = 0
		interpMatDate = addMonths(tempDate,3)	# add 3 months
		interpQuote = 0.0
		# iterate through all the futures curve
		for tempFuture in tempFutCurve:
			if (tempFuture.matDate-interpMatDate).days >= 0:
				# Add a quote - # RECHECK THIS
				slope = (tempFuture.quotePrice - prevQuote)/float((tempFuture.matDate - prevMat).days)
				intersect = prevQuote
				interpQuote = slope*float((interpMatDate - prevMat).days) + intersect
				# print "prevQuote: " + str(prevQuote) + "  currQuote: " + str(tempFuture.quotePrice) + "  interpQuote: " + str(interpQuote) + " date diff: " + str((interpMatDate - prevMat).days)
				adjQuoteArray.append((interpMatDate, interpQuote))

				interpMatDate = addMonths(interpMatDate,3)	# add 3 months
				index = index + 1

			prevQuote = tempFuture.quotePrice
			prevMat = tempFuture.matDate

		# deposit back into the database using the same asset_ids as before
		# grab the asset ids in order of maturity
		sqlString = """
		SELECT fq.futures_code, fq.asset_id
			FROM [SHA2].[rates_data].[futures_quotes] fq 
			LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id
			LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
			WHERE att.asset_type_name = '{assetTypeName}' AND fq.save_date = '{sqlDate}'
		""".format(
			assetTypeName = curveName,
			sqlDate = tempDate.strftime('%Y-%m-%d')
			)
		cur.execute(sqlString)

		tempAssetIDArray = []
		# populate the assetIDs
		for row in cur:
			tempAssetIDArray.append(row.asset_id)

		# populate the dictionary
		for index, tempQuote in enumerate(adjQuoteArray):
			if valAddStr == "":
				valDelStr = str(tempAssetIDArray[index])
				valAddStr = "('" + tempDate.strftime('%Y-%m-%d') + "'," + str(tempAssetIDArray[index]) + ",'" + tempQuote[0].strftime('%Y-%m-%d') + "'," + str(tempQuote[1]) + ")"
			else:
				valDelStr = valDelStr + "," + str(tempAssetIDArray[index])
				valAddStr = valAddStr + ",('" + tempDate.strftime('%Y-%m-%d') + "'," + str(tempAssetIDArray[index]) + ",'" + tempQuote[0].strftime('%Y-%m-%d') + "'," + str(tempQuote[1]) + ")"

		# print valAddStr
		# Delete after all currencies processed to reduce access to the database
		if valDelStr != "":
			delSqlString = """
				DELETE FROM [SHA2].[rates_data].[futures_norm_quotes] 
					WHERE save_date = '{currDate}' 
					AND asset_id IN ({delAssetID})
				""".format(
					currDate = tempDate.strftime('%Y-%m-%d'),
					delAssetID = valDelStr
					)

		# print delSqlString
		cur.execute(delSqlString)

		# Insert after all currencies proccessed to reduce access to the database
		if valAddStr != "":
			sqlString = """
				INSERT INTO [SHA2].[rates_data].[futures_norm_quotes] 
					(save_date, asset_id, maturity_date, rate_quote)
					VALUES {valueString}
				""".format(
					valueString = valAddStr
					)
		
		# print sqlString
		cur.execute(sqlString)


####################################################################
################# Main processing module ###########################
####################################################################

runDate = ('{:%Y%m%d}'.format(datetime.now()))

if len(sys.argv) != 2:
	print "Usage: ipython STIRFuturesDatabaseFunctions.py <DATE>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

print str(runDate) + ": Calculating Normalized STIR Rates for "+ str(sys.argv[1])
calcConstantMaturityPrices(sys.argv[1])

print str(runDate) + ": Done for " + str(sys.argv[1])
# Exit without a problem
sys.exit(0)