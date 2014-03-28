from ShortRateFuturesOption import *

origPriceLM = 98.83
origPriceED = 99.37

origStrikeLM = 98.75
origStrikeED = 99.375

origPutPriceLM = 0.075
origPutPriceED = 0.065

# Declare the two options
putAprLM587 = ShortRateFuturesOption("GBP","June 20, 2015", origPriceLM, "March 7, 2014", "May 21, 2014", origStrikeLM, 0.005, origPutPriceLM, -1000, 1)
putAprEDM593 = ShortRateFuturesOption("USD","June 20, 2015", origPriceED, "March 7, 2014", "May 16, 2014", origStrikeED, 0.003, origPutPriceED, 1000, 1)

origPutPriceLM = putAprLM587.price
origPutPriceEDM = putAprEDM593.price

# Assumes all USD as currency matched on delta
# Shift is vs. rate +30 is +30bps yield
shiftArray = [-30,-15, -10, -5, 0, +5, +10, +15,+30]
wideArray = [-15,-10,-5,0,+5,+10,+15]

deltaTable = [[0 for x in range(len(wideArray))] for x in range(len(shiftArray))]
gammaTable = [[0 for x in range(len(wideArray))] for x in range(len(shiftArray))]
thetaTable = [[0 for x in range(len(wideArray))] for x in range(len(shiftArray))]
vegaTable = [[0 for x in range(len(wideArray))] for x in range(len(shiftArray))]
valueTable = [[0 for x in range(len(wideArray))] for x in range(len(shiftArray))]

for shiftIndex, shiftAmt in enumerate(shiftArray):

	# Generate deltas for parallel shift in both
	#print "\n\t####SCENARIO: " + str(shiftAmt) + " SHIFT IN RATE####"
	#print "\tSHIFT\tDELTA\tGAMMA\tTHETA\tVEGA\tVALUE\n"
	
	for wideIndex, wideAmt in enumerate(wideArray):

		# Convert to rate (-ve == +ve price adjustment)
		putAprLM587.setFutPrice(origPriceLM + (-1)*float(shiftAmt)/100 + (-1)*float(wideAmt)/100) 
		putAprEDM593.setFutPrice(origPriceED + (-1)*float(shiftAmt)/100)

		deltaTable[shiftIndex][wideIndex]=25*(putAprLM587.getDelta()+putAprEDM593.getDelta())
		gammaTable[shiftIndex][wideIndex]=25*(putAprLM587.getGamma()+putAprEDM593.getGamma())
		thetaTable[shiftIndex][wideIndex]=25*(putAprLM587.getTheta()+putAprEDM593.getTheta())
		vegaTable[shiftIndex][wideIndex]=25*(putAprLM587.getVega()+putAprEDM593.getVega())
		valueTable[shiftIndex][wideIndex]=25*100*((putAprLM587.price- origPutPriceLM)*putAprLM587.position + (putAprEDM593.price-origPutPriceEDM)*putAprEDM593.position)
		#deltaArray.append(25*(putAprLM587.getDelta()+putAprEDM593.getDelta()))
		#gammaArray.append(25*(putAprLM587.getGamma()+putAprEDM593.getGamma()))
		#thetaArray.append(25*(putAprLM587.getTheta()+putAprEDM593.getTheta()))
		#vegaArray.append(25*(putAprLM587.getVega()+putAprEDM593.getVega()))
		#valueArray.append(25*100*(putAprLM587.price*putAprLM587.position + putAprEDM593.price*putAprEDM593.position))

		#printStr = "\t{0:.0f}\t{1:.0f}\t{2:.0f}\t{3:.0f}\t{4:.0f}\t{5:,.0f}".format(wideAmt, deltaArray[-1], gammaArray[-1], thetaArray[-1], vegaArray[-1], valueArray[-1])
		#print printStr
		
		putAprLM587.setFutPrice(origPriceLM)
		putAprEDM593.setFutPrice(origPriceED)

#HEADER
print "\nDELTA"
printStr = "SHIFT\t\t"
for shiftIndex, shiftAmt in enumerate(shiftArray):
	printStr = printStr + "{0}\t".format(shiftAmt)

print printStr + "\nWIDE"

for wideIndex, wideAmt in enumerate(wideArray):
	printStr = "{0:.0f}\t\t".format(wideAmt)
	for shiftIndex, shiftAmt in enumerate(shiftArray):
		printStr = printStr + "{0:,.0f}\t".format(deltaTable[shiftIndex][wideIndex])

	print printStr

#HEADER
print "\nGAMMA"
printStr = "SHIFT\t\t"
for shiftIndex, shiftAmt in enumerate(shiftArray):
	printStr = printStr + "{0}\t".format(shiftAmt)

print printStr + "\nWIDE"

for wideIndex, wideAmt in enumerate(wideArray):
	printStr = "{0:.0f}\t\t".format(wideAmt)
	for shiftIndex, shiftAmt in enumerate(shiftArray):
		printStr = printStr + "{0:,.0f}\t".format(gammaTable[shiftIndex][wideIndex])

	print printStr

#HEADER
print "\nTHETA"
printStr = "SHIFT\t\t"
for shiftIndex, shiftAmt in enumerate(shiftArray):
	printStr = printStr + "{0}\t".format(shiftAmt)

print printStr + "\nWIDE"

for wideIndex, wideAmt in enumerate(wideArray):
	printStr = "{0:.0f}\t\t".format(wideAmt)
	for shiftIndex, shiftAmt in enumerate(shiftArray):
		printStr = printStr + "{0:,.0f}\t".format(thetaTable[shiftIndex][wideIndex])

	print printStr

#HEADER
print "\nVEGA"
printStr = "SHIFT\t\t"
for shiftIndex, shiftAmt in enumerate(shiftArray):
	printStr = printStr + "{0}\t".format(shiftAmt)

print printStr + "\nWIDE"

for wideIndex, wideAmt in enumerate(wideArray):
	printStr = "{0:.0f}\t\t".format(wideAmt)
	for shiftIndex, shiftAmt in enumerate(shiftArray):
		printStr = printStr + "{0:,.0f}\t".format(vegaTable[shiftIndex][wideIndex])

	print printStr

#HEADER
print "\nVALUE ('000s)"
printStr = "SHIFT\t\t"
for shiftIndex, shiftAmt in enumerate(shiftArray):
	printStr = printStr + "{0}\t".format(shiftAmt)

print printStr + "\nWIDE"

for wideIndex, wideAmt in enumerate(wideArray):
	printStr = "{0:.0f}\t\t".format(wideAmt)
	for shiftIndex, shiftAmt in enumerate(shiftArray):
		printStr = printStr + "{0:,.1f}\t".format(valueTable[shiftIndex][wideIndex]/1000)

	print printStr