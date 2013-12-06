from dateutil import parser
from datetime import *
import pyodbc

# Define basic class for short rate futures to be used more as a container
class ShortRateFutures(object):
	# Calendar codes corresponding to the month for IMM
	calCodes = {3:'H', 6:'M', 9:'U', 12:'Z'}
	def __init__(self, ccyCode, matDate, saveDate, quotePrice = 100.0, position = 0):
		# Currency code for calendar and curve identification
		self.ccyCode = ccyCode.upper()
		# Using the date type
		self.matDate = parser.parse(matDate)
		# Set current date (for price)
		self.saveDate = parser.parse(saveDate)
		# Price setting
		self.quotePrice = float(quotePrice)
		# Futures code
		self.futCode = self.calCodes[self.matDate.month] + str(self.matDate.year%10)
		# Position if specified
		self.position = int(position)


# Define the curve class that takes in all the securities from the database
class ShortRateFuturesCurve(object):
	def __init__(self, curveName):
		self.curveName = curveName.upper()
		self.ccyCode = curveName[0:3].upper()
		self.saveDate = date.today()

	def __iter__(self):
		for tempFut in self.shortRateCurveQuotes:
			yield tempFut

	# mostly a read for getting prices
	def __getitem__(self, key):
		if isinstance(key,basestring):
			for tempFut in self.shortRateCurveQuotes:
				if tempFut.futCode == str(key).upper():
					return tempFut
			# return last tempFut
			raise KeyError
		else:
			raise TypeError

	# set an appropriate key
	def __setitem__(self,key,item):
		if isinstance(key,basestring):
			if isinstance(item,ShortRateFutures):
				for i, tempFut in enumerate(self.shortRateCurveQuotes):
					if tempFut.futCode == item.futCode:
						tempFut = item
						self.shortRateCurveQuotes[i] = tempFut
						return tempFut
				raise KeyError
			else:
				raise TypeError
		else:
			raise TypeError

	def loadCurveQuotes(self, saveDate):
		self.shortRateCurveQuotes = []
		db = self.msSQLConnect()
		cur = db.cursor()
		self.saveDate = parser.parse(saveDate)
		qryString = """
		SELECT fq.save_date, fq.maturity_date, fq.rate_quote, fq.futures_code, fq.asset_id FROM [SHA2].[rates_data].[futures_quotes] fq 
			LEFT JOIN [SHA2].[rates_data].[asset_table] at ON fq.asset_id = at.asset_id 
			LEFT JOIN [SHA2].[rates_data].[asset_type_table] att ON at.asset_type_id = att.asset_type_id
			WHERE att.asset_type_name = '{curveName}' AND fq.save_date = '{saveDateOut}' ORDER BY fq.maturity_date
			""".format(
				curveName = self.curveName,
				saveDateOut = self.saveDate.strftime('%Y-%m-%d')
				)
		cur.execute(qryString)
		for row in cur:
			tempFuture = ShortRateFutures(self.ccyCode,row.maturity_date, row.save_date, row.rate_quote)
			self.shortRateCurveQuotes.append(tempFuture)

		# print(qryString)
		return self.shortRateCurveQuotes


	def msSQLConnect(self):
		dbConnectionStr = "DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes"
		try:
		    db = pyodbc.connect(dbConnectionStr, autocommit = True)
		except pyodbc.Error, err:
		    logging.warning("Database connection error. " + str(err))
		    sys.exit()
		return db

