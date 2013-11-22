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