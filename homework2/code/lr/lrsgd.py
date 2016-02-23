#!/usr/bin/env python
"""
Implement your own version of logistic regression with stochastic
gradient descent.

Author: Alex Milkov
Email : amilkov3@gmail.com
"""

import collections
import math
import sys 


class LogisticRegressionSGD:

    def __init__(self, eta, mu, n_feature):
        """
        Initialization of model parameters  
        """
        self.eta = eta
        self.weight = [0.0] * n_feature
        self.mu = mu

    def fit(self, X, y):
        """
        Update model using a pair of training sample
        """

        #dic = self.to_dict(X)
        #print dic 
        #self.weight[self.idxs(X)] = self.weight[self.idxs(X)] - self.eta * ((self.predict_prob(X) - y)*self.values(X) - self.mu*abs(self.weight[self.idxs(X)])**2)
        self.weight[self.idxs(X)] = self.weight[self.idxs(X)] + self.eta * ((y - self.predict_prob(X))*self.values(X) - self.mu*abs(self.weight[self.idxs(X)])**2)
        #for f, v in dic:
        #    self.weight[f] = self.weight[f] - self.eta * ((y - self.predict_prob(X))*v - self.mu*abs(self.weight[f])**2)
        #for i in range(len(X)):
        #   self.weight[X[i][0]] = self.weight[X[i][0]] - self.eta * ((self.predict_prob([X[i]]) - self.y(y))*X[i][1] - self.mu*abs(self.weight[X[i][0]])**2)
       

    def predict(self, X):
        return 1 if self.predict_prob(X) > 0.5 else 0

    def predict_prob(self, X):
        return 1.0 / (1.0 + math.exp(-math.fsum((self.weight[f]*v for f, v in X))))

    def values(self, X):
        for f, v in X:
            return v

    def idxs(self, X):
        for f, v in X:
            return f

    def y(self, y):
        return y

    def to_dict(self, X):
        dic = {}
        for f, v in X:
            dic[f] = v
        return dic         
