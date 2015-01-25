
    
import pandas as pd
import numpy as np
import datetime
import zipline
from zipline.algorithm import TradingAlgorithm   
from zipline.transforms import batch_transform



# Stocks we will test our algorithm on.
STOCKS = ['JPM', 'S','VZ','AAPL','F','AA','KFRC']

@batch_transform
def anticor_matrix_calc( data, days_num, current_port ):
    LX_full = pd.DataFrame(data = data.price.values[1:]/data.price.values[:-1], columns= data.price.columns, index = data.price.index[1:])
    current_port = pd.TimeSeries(index = LX_full.columns, data= current_port )
    thresh = 0
    neg_thresh = 0
    LX_log = np.log(LX_full)    
    window = len(LX_log)/2
    LX2 = LX_log[-window:]
    LX1 = LX_log[:window]
    mu1 = LX1.mean()
    mu2 = LX2.mean()
    sig1 = LX1.std()
    sig2 = LX2.std()
    num_assets = len(data.price.columns)
    Mcorr = pd.DataFrame(index = LX_full.columns, columns = LX_full.columns, data= np.matrix((((LX1-LX1.mean())/LX1.std()).transpose().values))*np.matrix((((LX2-LX2.mean())/LX2.std()).values))/(window-1))
    claim = pd.DataFrame(index = LX_full.columns, columns = LX_full.columns, data= np.matrix(np.zeros((num_assets,num_assets))))
    Mu_check = np.matrix(np.zeros((num_assets,num_assets)))
    for i in range(num_assets):
        for j in range(num_assets):
            if mu2[i] > mu2[j] :
                Mu_check[i,j]= 1  
    Mcor_dag_neg = np.zeros(num_assets)
    for i in range(num_assets):
        if (Mcorr.values.diagonal()[i] <neg_thresh):
            Mcor_dag_neg[i] = np.abs(Mcorr.values.diagonal()[i])
    A = (np.repeat(Mcor_dag_neg,num_assets)).reshape((num_assets,num_assets))
    Addition = A+A.transpose()
    claim[(Mcorr >thresh) & (Mu_check > 0)] = Mcorr[(Mcorr >thresh) & (Mu_check > 0)]
    Add_pd = pd.DataFrame(data=Addition, index=LX_full.columns, columns=LX_full.columns)
    claim[(Mcorr >thresh) & (Mu_check > 0)] = claim[(Mcorr >thresh) & (Mu_check > 0)] + Add_pd[(Mcorr >thresh) & (Mu_check > 0)]
    claim_sum = pd.DataFrame(data= np.repeat(claim.sum(axis=1).values, num_assets).reshape((num_assets,num_assets)),index = LX_full.columns, columns= LX_full.columns)
    transfer = pd.DataFrame(data = np.zeros((num_assets,num_assets)),index = LX_full.columns, columns= LX_full.columns)    
    transfer[claim_sum !=0]  = claim[claim_sum !=0]/claim_sum[claim_sum !=0]
    port_tilde = (LX_full[-1:]*current_port).values/np.sum((LX_full[-1:]*current_port).values)
    transfer  =transfer *np.repeat(port_tilde[0],num_assets,axis=0).reshape((num_assets,num_assets))   
    new_portfolio = port_tilde[0] + transfer.sum(axis=0) -transfer.sum(axis=1)
    new_portfolio[new_portfolio < 0.0000001] = 0    
    return new_portfolio.values

# Actual zipline algorithm.
class ANTICOR(TradingAlgorithm):

    def initialize(self,eps=1, window_length=2):
        self.stocks = STOCKS
        self.m = len(self.stocks)
        self.current_port = np.ones(self.m) / self.m # Current portfolio composition in weights of inidividual stocks
        self.init = True
        self.days = 0
        self.window_length = window_length
        self.eps =eps
        self.anticor_matrix_calc = anticor_matrix_calc(refresh_period=0,window_length=2*self.window_length+1)

        self.set_slippage(zipline.finance.slippage.FixedSlippage())
        self.set_commission(zipline.finance.commission.PerShare(cost=0))
        
    def handle_data(self, data):
        self.days += 1
        #print self.days
        
        #???
        if self.days < 2*self.window_length:
            return
    
        if self.init:
            self.rebalance_portfolio( data, self.current_port )
            self.init = False
            return
        
        #print data['JPM'].datetime
        b_norm = self.anticor_matrix_calc.handle_data( data, self.days, self.current_port )
        if b_norm != None:
            self.current_port = b_norm
            self.rebalance_portfolio( data, self.current_port )
            #print self.portfolio.portfolio_value
        #print b_norm
        
    def rebalance_portfolio(self, data, desired_port):
        #rebalance portfolio
        if self.init:
            positions_value = self.portfolio.starting_cash
        else:
            positions_value = self.portfolio.positions_value + self.portfolio.cash
    
        current_amount = np.zeros_like( desired_port )
        prices = np.zeros_like( desired_port )
        for i, stock in enumerate(self.stocks):
            current_amount[i] = self.portfolio.positions[stock].amount
            prices[i] = data[stock].price
    
        desired_amount = np.round( desired_port * positions_value / prices )
        diff_amount = desired_amount - current_amount

        for i, stock in enumerate( self.stocks ):
            self.order( stock, diff_amount[i] ) #order_stock
        

if __name__ == '__main__' :
    start = datetime.datetime(2012, 1, 15)
    end = datetime.datetime(2015, 1, 15)
    data = zipline.data.load_from_yahoo(stocks=STOCKS, indexes={}, start=start, end=end)
    data = data.dropna()
    
    #print data
    portfolio_value = []
    for window_size in np.arange(4,60,1):
        anti=ANTICOR(0,window_size)
        ptf = anti.run(data)
        portfolio_value.append(ptf['portfolio_value'][-1])
    print 'Done'