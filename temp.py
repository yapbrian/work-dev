import numpy as np 
import matplotlib.pyplot as plt 
import math

tauLevel = np.array([(x+1)/100.0 for x in range(3000)])

lambda1 = [(x+1)/10.0 for x in range(25)]
beta2 = [(x+1)/100.0 for x in range(125)]

colorMap = plt.get_cmap("Reds")
graphColor = [ colorMap(float(i)/len(beta2)) for i in xrange(len(beta2)) ]

for i,tempB2 in enumerate(beta2):
	TOverLambda = tauLevel / 2.5
	expTOverLambda = np.array([math.exp(-x) for x in TOverLambda])
	plotFunc = 0.5*(1-expTOverLambda)/TOverLambda+tempB2*((1-expTOverLambda)/TOverLambda - expTOverLambda)

	plt.plot(tauLevel, plotFunc, c=graphColor[i])

