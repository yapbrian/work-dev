from dateutil import parser
from datetime import *
from scipy.interpolate import interp1d
import pyodbc
import sys
from GovernmentBonds import *

ccyToBondCurveMap = {'USD':'USD_CASH_BOND_GOVT_BMK', 'CAD':'CAD_CASH_BOND_GOVT_BMK', 'GBP':'GBP_CASH_BOND_GOVT_BMK', 'EUR':'DEM_CASH_BOND_GOVT_BMK', 'AUD':'AUD_CASH_BOND_GOVT_BMK'}

def msSQLConnect():
	dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
	try:
		db = pyodbc.connect(dbConnectionStr, autocommit = True)
	except pyodbc.Error, err:
		logging.warning("Database connection error. " + str(err))
		sys.exit()
	return db

# Grab a bond curve and interpolate between on the runs to create a constant maturity government bond yield
def fillInterpYields(saveDate):
	db = msSQLConnect()
	cur = db.cursor()
	saveDate = parser.parse(saveDate)

	addString = ""
	delString = ""

	for ccyCode, curveName in ccyToBondCurveMap.items():
		curveName = curveName.upper()
		ccyCode = ccyCode.upper()
		strSql = """
		SELECT bb.bond_benchmark_id, bb.bond_benchmark_name 
			FROM [SHA2].[rates_data].[bond_benchmark] bb
			LEFT JOIN [SHA2].[rates_data].[bond_benchmark_curve] bbc ON bb.bond_benchmark_curve_id = bbc.bond_benchmark_curve_id
			WHERE bbc.bond_benchmark_curve_name = '{sqlCurveName}'
			""".format(
				sqlCurveName = curveName
				)
		cur.execute(strSql)

		matArray = []
		idArray = []
		# Parse for maturities
		for row in cur:
			splitName = row.bond_benchmark_name.split("_")
			matYear = splitName[-1]	# Last item is maturity identifier
			matYear = int(matYear[:-1])
			idArray.append(row.bond_benchmark_id)
			matDate = datetime(saveDate.year+matYear, saveDate.month, saveDate.day)
			matArray.append(matDate)

		bondCurveInterp = GovernmentBondCurve(ccyCode, curveName)
		bondCurveInterp.loadBondBmkQuotes(saveDate.strftime("%Y-%m-%d"))

		interpYields = bondCurveInterp.interpBondBmk(matArray)

		for i,matDate in enumerate(matArray):
			tempString = "('{0}',{1},'{2}',{3})".format(saveDate.strftime("%Y-%m-%d"),interpYields[i], matDate.strftime("%Y-%m-%d"),idArray[i]) 
			if addString == "":
				addString = tempString
				delString = str(idArray[i])
			else:
				addString = addString + "," + tempString
				delString = delString + "," + str(idArray[i])

	sqlString = "DELETE FROM [SHA2].[rates_data].[bond_norm_benchmark_quotes] WHERE save_date = '{0}' AND bond_benchmark_id IN ({1})".format(saveDate.strftime("%Y-%m-%d"),delString)
	cur.execute(sqlString)

	sqlString = "INSERT INTO [SHA2].[rates_data].[bond_norm_benchmark_quotes] (save_date,yield_to_maturity,maturity_date,bond_benchmark_id) VALUES {0}".format(addString)
	cur.execute(sqlString)


####################################################################
################# Main processing module ###########################
####################################################################

runDate = ('{:%Y%m%d}'.format(datetime.now()))

if len(sys.argv) != 2:
	print "Usage: ipython BondsDatabaseFunctions.py <DATE>"
	print "<DATE> is in YYYYMMDD format"
	sys.exit(1)

print str(runDate) + ": Calculating Normalized Bonds Rates for "+ str(sys.argv[1])
fillInterpYields(sys.argv[1])

print str(runDate) + ": Done for " + str(sys.argv[1])
# Exit without a problem
sys.exit(0)


