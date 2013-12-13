import math
from scipy.stats import norm
 
 # using Arithmetic Brownian Motion
class BlackNormalOptionABMPricer(object):
    # putcall flag, 0 = call, 1 = put
    # k as strike, s as current price
    # 
    def __init__(self, k, s, rf, vol, t, putcall = 0):
        self.k = float(k)
        self.s = float(s)
        self.rf = float(rf)
        self.vol = float(vol)
        self.t = float(t)
        self.putcall = bool(putcall)
 
    def getPrice(self):
        d = ((100-self.k) - (100-self.s))/(self.vol*math.pow(self.t,0.5))
        callPrice = math.exp(-self.rf*self.t)*((100-self.k) - (100-self.s))*norm.cdf(d) + self.vol*norm.pdf(d)*math.pow(self.t,0.5)
        # if call
        if self.putcall:
            return callPrice -(math.exp(-self.rf*self.t)*(self.s-self.k))
        else:
            return callPrice

    # Analytic formula for delta under black normal formula
    def getDelta(self):
        d = ((100-self.k) - (100-self.s))/(self.vol*math.pow(self.t,0.5))
        # Norm.cdf(-d) for a put
        if self.putcall: d = -d
        return math.exp(-self.rf*self.t)*norm.cdf(d)

    def getTheta(self, dt = 0.01):
        self.t += dt
        afterPrice = self.getPrice()
        self.t -= dt
        origPrice = self.getPrice()
        return (afterPrice - origPrice) / dt * -1

    # Analytic formula for Gamma under black normal formula
    def getGamma(self):
        d = ((100-self.k) - (100-self.s))/(self.vol*math.pow(self.t,0.5))
        return math.exp(-self.rf*self.t)/(self.vol*math.pow(self.t,0.5))*norm.pdf(d)

    def getVega(self):
        d = ((100-self.k) - (100-self.s))/(self.vol*math.pow(self.t,0.5))
        return math.exp(-self.rf*self.t)*math.pow(self.t,0.5)*norm.pdf(d)

    def getImpVol(self, solvePrice, MAX_ITER=1000, ACC=1.0e-5):
        """Calculates implied volatility for the Black Scholes formula using
        the Newton-Raphson formula
        Converted to Python from "Financial Numerical Recipes in C" by:
        Bernt Arne Odegaard
        http://finance.bi.no/~bernt/gcc_prog/index.html
        (NOTE: In the original code a large negative number was used as an
        exception handling mechanism.  This has been replace with a generic
        'Exception' that is thrown.  The original code is in place and commented
        if you want to use the pure version of this code)
        """                                 
        # check for arbitrage violations. Option price is too low if this happens
        tempExec = (self.k - self.s) if self.putcall else (self.s - self.k)
        if (solvePrice<0.99*(math.exp(-self.rf*self.t)*tempExec)): 
            # print "Arb condition violated : " + str(0.99*(math.exp(-self.rf*self.t)*tempExec))
            return 0.0

        guessVol = 0.5 #(solvePrice/self.s)/(0.398*self.t) # find initial value

        d = ((100-self.k) - (100-self.s))/(guessVol*math.pow(self.t,0.5))

        for i in range(MAX_ITER):
            
            callPrice = math.exp(-self.rf*self.t)*((100-self.k) - (100-self.s))*norm.cdf(d) + guessVol*norm.pdf(d)*math.pow(self.t,0.5)
            
            price =  (callPrice - (math.exp(-self.rf*self.t)*(self.s-self.k))) if self.putcall else callPrice
            diff = solvePrice - price

            if (abs(diff)<ACC):
                return guessVol
            
            d = ((100-self.k) - (100-self.s))/(guessVol*math.pow(self.t,0.5))
            vega = math.exp(-self.rf*self.t)*math.pow(self.t,0.5)*norm.pdf(d)
            guessVol = guessVol + diff/vega
        #return -99e10 # something screwy happened, should throw exception // <--- original code
        raise Exception("An error occurred") # Comment this line if you uncomment the line above  

    # def getDelta(self, d = 0.01):
    #    self.s += d
    #    afterPrice = self.getPrice()
    #    self.s -= 2*d
    #    origPrice = self.getPrice()
    #    self.s += d
    #    return (afterPrice - origPrice) / (2*d)

    # def getGamma(self, d=0.01):
    #    self.s += d
    #    afterDelta = self.getDelta()
    #    self.s -= 2*d
    #    origDelta = self.getDelta()
    #    self.s += d
    #    return (afterDelta - origDelta) / (2*d)

    # def getVega(self, d=0.01):
    #    self.vol += d
    #    afterPrice = self.getPrice()
    #    self.vol -= 2*d
    #    origPrice = self.getPrice()
    #    self.vol += d
    #    return (afterPrice - origPrice) / (2*d) * -1

    # def option_price_implied_volatility_call_black_scholes_bisections(S, K, r, time,
    #                                                               option_price):
    #     """Calculates implied volatility for the Black Scholes formula using
    #     binomial search algorithm
    #     Converted to Python from "Financial Numerical Recipes in C" by:
    #     Bernt Arne Odegaard
    #     http://finance.bi.no/~bernt/gcc_prog/index.html
    #     (NOTE: In the original code a large negative number was used as an
    #     exception handling mechanism.  This has been replace with a generic
    #     'Exception' that is thrown.  The original code is in place and commented
    #     if you want to use the pure version of this code)
    #     @param S: spot (underlying) price
    #     @param K: strike (exercise) price,
    #     @param r: interest rate
    #     @param time: time to maturity 
    #     @param option_price: The price of the option
    #     @return: Sigma (implied volatility)
    #     """                                                             
    #     if (option_price<0.99*(S-K*exp(-time*r))):  # check for arbitrage violations. 
    #         return 0.0                           # Option price is too low if this happens
      
    #     # simple binomial search for the implied volatility.
    #     # relies on the value of the option increasing in volatility
    #     ACCURACY = 1.0e-5 # make this smaller for higher accuracy
    #     MAX_ITERATIONS = 100
    #     HIGH_VALUE = 1e10
    #     #ERROR = -1e40  // <--- original code
      
    #     # want to bracket sigma. first find a maximum sigma by finding a sigma
    #     # with a estimated price higher than the actual price.
    #     sigma_low=1e-5
    #     sigma_high=0.3
    #     price = option_price_call_black_scholes(S,K,r,sigma_high,time)
    #     while (price < option_price):  
    #         sigma_high = 2.0 * sigma_high # keep doubling.
    #         price = option_price_call_black_scholes(S,K,r,sigma_high,time)
    #         if (sigma_high>HIGH_VALUE):
    #             #return ERROR # panic, something wrong.  // <--- original code
    #             raise Exception("panic, something wrong.") # Comment this line if you uncomment the line above

    #     for i in xrange(0, MAX_ITERATIONS):
    #         sigma = (sigma_low+sigma_high)*0.5
    #         price = option_price_call_black_scholes(S,K,r,sigma,time)
    #         test = (price-option_price)
    #         if (abs(test)<ACCURACY):
    #             return sigma
    #         if (test < 0.0):
    #             sigma_low = sigma
    #         else:
    #             sigma_high = sigma
    #     #return ERROR      // <--- original code
    #     raise Exception("An error occurred") # Comment this line if you uncomment the line above
              