import RatesDataBaseQueryFunctions as rdbqf
import sys
from datetime import *
from dateutil import parser

# Calc the factor sensitivities to 2-5-10-30s for the curve listed (by deltas)
# endDate is used as asOfDate
def generateCurveSensitivityFactor(startDate, endDate, curveName, factorAssetList):
	# Get all the asset_ids related to the curveName - which should be swap curve points
	db = rdbqf.msSQLConnect()
	cur = db.cursor()
	assetIDDict = {}

	# String to upload into sql database
	valueStr = ""
	delIDStr = ""

	# Only run on spot
	qryString = """
		SELECT at.asset_id, at.asset_name FROM SHA2.rates_data.asset_table at
		LEFT JOIN SHA2.rates_data.asset_type_table att ON at.asset_type_id = att.asset_type_id
		WHERE att.asset_type_name = '{curveNameSQL}'
		AND at.asset_name LIKE '%_SPOT_%'
		""".format(
			curveNameSQL = curveName)

	cur.execute(qryString)

	for row in cur:
		assetIDDict[row.asset_name] = row.asset_id

	for keyName, keyID in assetIDDict.items():
		# print "Working " + keyName
		print keyName
		print factorAssetList
		if keyName in factorAssetList:
			# only one exposure - itself
			tmpValStr = "(" + str(keyID) + ",'" + endDate.strftime('%Y-%m-%d') + "'," + str(keyID) + ",1.00, 1)" 

			if valueStr == "":
				valueStr = tmpValStr
				delIDStr = str(keyID)
			else:
				valueStr = valueStr + "," + tmpValStr
				delIDStr = delIDStr + "," + str(keyID)
		
		else:
			regrResult = rdbqf.regressAssetData(startDate, endDate, keyName, factorAssetList, onDeltas=True)

			# Exclude the intercept
			for tmpIndex in range(len(factorAssetList)):
				tmpValStr = "(" + str(keyID) + ",'" + endDate.strftime('%Y-%m-%d') + "'," + str(assetIDDict[regrResult.x.columns[tmpIndex]]) + "," + str(regrResult.beta[tmpIndex]) + ", 1)" 

				if valueStr == "":
					valueStr = tmpValStr
					delIDStr = str(keyID)
				else:
					valueStr = valueStr + "," + tmpValStr
					delIDStr = delIDStr + "," + str(keyID)

	# Still have to delete assetIDs entries based on save date and assetID (not factor assetID)
	sqlInsertStr = """
		INSERT INTO [SHA2].[rates_data].[rates_curve_factor_sensitivity] 
		(asset_id, save_date, factor_asset_id, factor_quote, factor_scenario_id) 
		VALUES {valueStrSQL}
		""".format(
			valueStrSQL = valueStr)

	sqlDeleteStr = """
		DELETE FROM [SHA2].[rates_data].[rates_curve_factor_sensitivity]
		WHERE save_date = '{endDateSQL}'
		AND asset_id IN ({delIDStrSQL})
		""".format(
			endDateSQL = endDate.strftime('%Y-%m-%d'),
			delIDStrSQL = delIDStr)

	cur.execute(sqlDeleteStr)
	cur.execute(sqlInsertStr)


####################################################################
################# Main processing module ###########################
####################################################################

runDate = ('{:%Y%m%d}'.format(datetime.now()))

if len(sys.argv) != 3:
	print "Usage: ipython FixedIncomePortfolio.py <DATE> <WINDOWDAYS>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

# curveName = sys.argv[1].upper()
endDate = parser.parse(sys.argv[1])
windowSize = int(sys.argv[2])
startDate = endDate + timedelta(days=-windowSize)

curveName = "USD_LIBOR_3M_SWAP"
factorAssetList = ["USD_LIBOR_3M_SWAP_SPOT_2Y","USD_LIBOR_3M_SWAP_SPOT_5Y","USD_LIBOR_3M_SWAP_SPOT_10Y","USD_LIBOR_3M_SWAP_SPOT_30Y"]
print "Running factor generation for " + runDate + " Curve: " + curveName
generateCurveSensitivityFactor(startDate, endDate, curveName, factorAssetList)
print "Done"

curveName = "GBP_LIBOR_3M_SWAP"
factorAssetList = ["GBP_LIBOR_3M_SWAP_SPOT_2Y","GBP_LIBOR_3M_SWAP_SPOT_5Y","GBP_LIBOR_3M_SWAP_SPOT_10Y","GBP_LIBOR_3M_SWAP_SPOT_30Y"]
print "Running factor generation for " + runDate + " Curve: " + curveName
generateCurveSensitivityFactor(startDate, endDate, curveName, factorAssetList)
print "Done"

curveName = "EUR_EURIBOR_3M_SWAP"
factorAssetList = ["EUR_EURIBOR_3M_SWAP_SPOT_2Y","EUR_EURIBOR_3M_SWAP_SPOT_5Y","EUR_EURIBOR_3M_SWAP_SPOT_10Y","EUR_EURIBOR_3M_SWAP_SPOT_30Y"]
print "Running factor generation for " + runDate + " Curve: " + curveName
generateCurveSensitivityFactor(startDate, endDate, curveName, factorAssetList)
print "Done"

curveName = "CAD_CDOR_3M_SWAP"
factorAssetList = ["CAD_CDOR_3M_SWAP_SPOT_2Y","CAD_CDOR_3M_SWAP_SPOT_5Y","CAD_CDOR_3M_SWAP_SPOT_10Y","CAD_CDOR_3M_SWAP_SPOT_30Y"]
print "Running factor generation for " + runDate + " Curve: " + curveName
generateCurveSensitivityFactor(startDate, endDate, curveName, factorAssetList)
print "Done"


