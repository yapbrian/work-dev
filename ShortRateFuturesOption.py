from BlackNormalOptionABMPricer import *
from ShortRateFutures import *

# Define basic holding class for ShortRateFuturesOption
class ShortRateFuturesOption(object):

	# maturity of the option
	def __init__(self, ccyCode, underFutMat, underFutPrice, saveDate, optMatDate, optStrike, optRf, optPrice, optPos, putcall = 0 ):
		# underlying futures object
		self.underlyingFut = ShortRateFutures(ccyCode, underFutMat, saveDate, underFutPrice)
		# makes it easier to get the underlying fut code
		self.__underFutCode = self.underlyingFut.futCode
		# maturity date of the option
		self.__matDate = parser.parse(optMatDate)
		# save date of the option
		self.__saveDate = parser.parse(saveDate)
		# strike of the option
		self.__strike = float(optStrike)
		# risk free rate
		self.__rf = float(optRf)
		# price of option (usually specify the price instead of the vol)
		self.__price = float(optPrice)
		# position in this option (note that this is units of option and not notional)
		self.position = int(optPos)
		# put or call 0 = call (false), 1 = put (true)
		self.__putcall = bool(putcall)
		# volatility - usually unspecified so imply from price with a temp option
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, 1.00, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		self.__vol =  tempOption.getImpVol(self.__price)

	# Helper functions to reprice option on property access
	def rePriceOption(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		self.__price = tempOption.getPrice()		

	# Helper function to reprice vol on property access
	def reVolOption(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		self.__vol = tempOption.getImpVol(self.__price)

	# get revised price given new underlying and volatility, or just current price
	def setFutPrice(self, underFutPrice):
		self.underlyingFut = ShortRateFutures(self.underlyingFut.ccyCode, str(self.underlyingFut.matDate), str(self.underlyingFut.saveDate), float(underFutPrice))

		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		self.__price = tempOption.getPrice()
		return self.__price

	def getDelta(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		return (-1)*tempOption.getDelta() * self.position	# get to rate risk instead of price risk so mult by -1

	def getGamma(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		return tempOption.getGamma() * self.position / 100 # adjustment to get per bp

	def getVega(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		return tempOption.getVega() * self.position # adjustment to get per bp not necessary since we look at price per bp (price needs to be adj)

	def getTheta(self):
		tempOption = BlackNormalOptionABMPricer(self.__strike, self.underlyingFut.quotePrice, self.__rf, self.__vol, float((self.__matDate-self.__saveDate).days)/365, self.__putcall)
		return tempOption.getTheta(1.0/365) * self.position * 100  # x100 to get in basis points pricing i.e. 0.05 = 5 bps

	def get_underFutCode(self):
		return self.__underFutCode

	def get_matDate(self):
		return self.__matDate

	def set_matDate(self, value):
		self.__matDate = parser.parse(value)
		self.rePriceOption()

	def get_saveDate(self):
		return self.__saveDate

	def set_saveDate(self, value):
		self.__saveDate = parser.parse(value)
		self.rePriceOption()

	def get_strike(self):
		return self.__strike

	def set_strike(self, value):
		self.__strike = float(value)

	def get_rf(self):
		return self.__rf

	def set_rf(self, value):
		self.__rf = float(value)
		self.rePriceOption()

	def get_price(self):
		return self.__price 

	def set_price(self, value):
		self.__price = float(value)
		self.reVolOption()

	def get_putcall(self):
		return self.__putcall

	def set_putcall(self, value):
		self.__putcall = bool(value)
		self.rePriceOption()

	def get_vol(self):
		return self.__vol

	def set_vol(self, value):
		self.__vol = float(value)
		self.rePriceOption()
	
	underFutCode = property(get_underFutCode)
	matDate = property(get_matDate, set_matDate)
	saveDate = property(get_saveDate, set_saveDate)
	strike = property(get_strike, set_strike)
	rf = property(get_rf, set_rf)
	price = property(get_price, set_price)
	putcall = property(get_putcall, set_putcall)
	vol = property(get_vol, set_vol)






