
import run_config
import datetime as dt
import zipline
from algo.factory import createAlgo


class BackTestRunner( object ):
    
    '''
    Each runner is reponsible for multiple runs of the algorithm (different portfolios, different parameters)
    within a given period (startDate, endDate)
    '''
    
    def __init__ ( self, algoName, allStocks ) :
        self._algoName  = algoName
        self._allStocks = allStocks
        self._startDate = None
        self._endDate   = None
        
        
    def run( self, startDate, endDate, *args, **kwargs ) :
        # Initialize algorithm
        algo = createAlgo( self.algoName, args, kwargs )
        
        if startDate != self._startDate || endDate != self._endDate :
            \self._data = zipline.data.load_from_yahoo( stocks=self._allStocks, indexes={}, start=start, end=end )
            self._data = data.dropna()
            
    def getResults( self ) :
        
            
class BackTestDriver( object ):
    
    def __init__( self, 
                   algo,              # name of algorithm
                   stockIndex,        # index from which we randomly pick assets 
                   nPortfolios,       # number of portfolios to try
                   nPeriods,          # number of periods on which to run the algorithm
                   endDate = None,    # default is today 
                   periodLen = 365 ): # default is 1Y 
        self._algo = algo
        self._portfolios = self._constructPortfolios( stockIndex, nPortfolios )
        self._endDate = endDate if endDate else dt.datetime.today()
        self._periodLen = periodLen
        self._periods = self._constructPeriods( nPeriods )
        
    def _constructPortfolios( self, stock_index, nPortfolios ):
        
    def _constructPeriods( self, nPeriods ):
        pass
    
    def run( self ):
        pass
    