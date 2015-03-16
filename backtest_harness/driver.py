
import datetime as dt
import random    
import itertools
     
from runner import AlgoRunner
import config


class PortfolioGenerator( object ) :
    
    '''
    Randomly sample portfolio of a given size from an dinex
    '''
    
    def __init__( self, 
                  index,          # index from which we randomly pick stocks
                  portSize,       # number of stocks in the portfolio
                  nPortfolios,    # number of portfolios to generator
                  seed = 123 ) :
        self._df_index = config.StockIndices[ index ]
        self._portSize = portSize
        self._i = 0
        self._n = nPortfolios
        random.seed( seed )
        
        
    def __iter__( self ):
        return self
    
    
    def next( self ):
        if self._i < self._n :
            self._i += 1
            return random.sample( self._df_index.index, self._portSize )
        else :
            raise StopIteration()
            
        
class BackTestDriver( object ):
    
    def __init__( self, 
                  algo,               # name of algorithm
                  params,             # ranges of algorithm params to explore
                  portfolios,         # different portflio compositions
                  nPeriods,           # number of periods on which to run the algorithm
                  endDate = None,     # default is today 
                  periodLen = 365 ) : # default is 1Y 
        self._algo = algo
        self._params = list( params )
        self._portfolios = list( portfolios )
        self._allStocks = self._collectAllStocks()
        self._endDate = endDate if endDate else dt.datetime.today().replace(hour = 0, minute = 0)
        self._periods = self._constructPeriods( nPeriods, periodLen )
        
    
    def _collectAllStocks( self ) :
        allStocks = set()
        for p in self._portfolios :
            for s in p :
                allStocks.add( s )
        return allStocks
    
        
    def _constructPeriods( self, nPeriods, periodLen ):
        periods = []
        endDate = self._endDate
        for i in xrange( nPeriods ) :
            startDate = endDate - dt.timedelta( days = periodLen )
            periods.append( (startDate, endDate) )
            endDate = startDate
        return periods
    
    
    def run( self ) :
        runner = AlgoRunner( self._algo, self._allStocks )
        
        self._results = []
        for s, e in self._periods :
            for port, params in itertools.product( self._portfolios, self._params ) :
                runner.run( s, e, port, params )
                
        self._results.extend( runner.results() )       
        
    