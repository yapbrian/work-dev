import math
from scipy.stats import norm
 
class Option:
    def __init__(self, k, s, rf, vol, t):
        self.k = float(k)
        self.s = float(s)
        self.rf = float(rf)
        self.vol = float(vol)
        self.t = float(t)
 
    def get_call(self):
        d1 = ( math.log(self.s/self.k) + ( self.rf + math.pow( self.vol, 2)/2 ) * self.t ) / ( self.vol * math.sqrt(self.t) )
        d2 = d1 - self.vol * math.sqrt(self.t)
        return ( norm.cdf(d1) * self.s - norm.cdf(d2) * self.k * math.exp( -self.rf * self.t ) )
 
    def get_delta(self, d = 0.01):
        self.s += d
        after_call_price = self.get_call()
        self.s -= d
        org_call_price = self.get_call()
        return (after_call_price - org_call_price) / d
 
    def get_theta(self, dt = 0.01):
        self.t += dt
        after_call_price = self.get_call()
        self.t -= dt
        org_call_price = self.get_call()
        return (after_call_price - org_call_price) / dt * -1

    def option_price_implied_volatility_call_black_scholes_bisections(S, K, r, time,
                                                                  option_price):
        """Calculates implied volatility for the Black Scholes formula using
        binomial search algorithm
        Converted to Python from "Financial Numerical Recipes in C" by:
        Bernt Arne Odegaard
        http://finance.bi.no/~bernt/gcc_prog/index.html
        (NOTE: In the original code a large negative number was used as an
        exception handling mechanism.  This has been replace with a generic
        'Exception' that is thrown.  The original code is in place and commented
        if you want to use the pure version of this code)
        @param S: spot (underlying) price
        @param K: strike (exercise) price,
        @param r: interest rate
        @param time: time to maturity 
        @param option_price: The price of the option
        @return: Sigma (implied volatility)
        """                                                             
        if (option_price<0.99*(S-K*exp(-time*r))):  # check for arbitrage violations. 
            return 0.0                           # Option price is too low if this happens
      
        # simple binomial search for the implied volatility.
        # relies on the value of the option increasing in volatility
        ACCURACY = 1.0e-5 # make this smaller for higher accuracy
        MAX_ITERATIONS = 100
        HIGH_VALUE = 1e10
        #ERROR = -1e40  // <--- original code
      
        # want to bracket sigma. first find a maximum sigma by finding a sigma
        # with a estimated price higher than the actual price.
        sigma_low=1e-5
        sigma_high=0.3
        price = option_price_call_black_scholes(S,K,r,sigma_high,time)
        while (price < option_price):  
            sigma_high = 2.0 * sigma_high # keep doubling.
            price = option_price_call_black_scholes(S,K,r,sigma_high,time)
            if (sigma_high>HIGH_VALUE):
                #return ERROR # panic, something wrong.  // <--- original code
                raise Exception("panic, something wrong.") # Comment this line if you uncomment the line above

        for i in xrange(0, MAX_ITERATIONS):
            sigma = (sigma_low+sigma_high)*0.5
            price = option_price_call_black_scholes(S,K,r,sigma,time)
            test = (price-option_price)
            if (abs(test)<ACCURACY):
                return sigma
            if (test < 0.0):
                sigma_low = sigma
            else:
                sigma_high = sigma
        #return ERROR      // <--- original code
        raise Exception("An error occurred") # Comment this line if you uncomment the line above
              
              
    def option_price_implied_volatility_call_black_scholes_newton(S, K, r, time,
                                                                  option_price):
        """Calculates implied volatility for the Black Scholes formula using
        the Newton-Raphson formula
        Converted to Python from "Financial Numerical Recipes in C" by:
        Bernt Arne Odegaard
        http://finance.bi.no/~bernt/gcc_prog/index.html
        (NOTE: In the original code a large negative number was used as an
        exception handling mechanism.  This has been replace with a generic
        'Exception' that is thrown.  The original code is in place and commented
        if you want to use the pure version of this code)
        @param S: spot (underlying) price
        @param K: strike (exercise) price,
        @param r: interest rate
        @param time: time to maturity 
        @param option_price: The price of the option
        @return: Sigma (implied volatility)
        """                                 
        if (option_price<0.99*(S-K*exp(-time*r))): # check for arbitrage violations. Option price is too low if this happens
            return 0.0

        MAX_ITERATIONS = 100
        ACCURACY = 1.0e-5
        t_sqrt = sqrt(time)

        sigma = (option_price/S)/(0.398*t_sqrt) # find initial value
        for i in xrange(0, MAX_ITERATIONS):
            price = option_price_call_black_scholes(S,K,r,sigma,time)
            diff = option_price -price
            if (abs(diff)<ACCURACY): 
                return sigma
            d1 = (log(S/K)+r*time)/(sigma*t_sqrt) + 0.5*sigma*t_sqrt
            vega = S * t_sqrt * n(d1)
            sigma = sigma + diff/vega
        #return -99e10 # something screwy happened, should throw exception // <--- original code
        raise Exception("An error occurred") # Comment this line if you uncomment the line above          