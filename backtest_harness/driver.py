
import datetime as dt
import random    
import itertools
from IPython.parallel.util import interactive
import sys
  
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
                  portfolios,         # different portflio compositions
                  params,             # ranges of algorithm params to explore
                  nPeriods = 1,       # number of periods on which to run the algorithm
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
        # Initialize a single runner since we are running locally
        runner = AlgoRunner( self._algo, self._allStocks )
        
        self._results = []
        for start, end in self._periods :
            nTasks = len( self._portfolios ) * len( self._params )
            for task_id, port, params in itertools.izip( xrange( nTasks ),
                                                         *zip( *itertools.product( self._portfolios, self._params ) ) ) :
                runner.run( task_id, start, end, port, params )
                
        self._processResults( runner.results() )    
        
        
    def run_parallel( self, clusterClient ) :
        '''
        clusterClient could be either local or remote
        '''
        
        # Create direct view  
        dview = clusterClient[:]
        dview.clear()
        #lview = clusterClient.load_balanced_view() 
        
        #@dview.remote(block = True)
        #def setupRunner( algoName, allStocks ) :
        #    from backtest_harness.runner import AlgoRunner
        #    runner = AlgoRunner( algoName, allStocks )
        #    return globals().keys()
        #print setupRunner( self._algo, self._allStocks )
        #sys.exit(1)
        
        # Import runner module and initialize a runner on each node
        cmd = "from backtest_harness.runner import AlgoRunner; runner = AlgoRunner('%s', %s)" % (self._algo, self._allStocks)
        dview.execute( cmd, block = True )
        
        remote_f = interactive( lambda t : runner.run( t[0], startDate, endDate, t[1], t[2] ) )
        
        for start, end in self._periods :
            dview['startDate'], dview['endDate'] = start, end
            
            nTasks = len( self._portfolios ) * len( self._params )
            taskGen = itertools.product( self._portfolios, self._params )
            
            # Non-blocking gather
            amr = dview.map_async( remote_f,
                                   itertools.izip( xrange( nTasks ), *zip( *taskGen ) ) ) # Tag each task with task id
                                   #ordered = False ) # Get results first-come-first, don't care about the order
            
            # Wait for results and process them as they arrive
            for r in amr :
                print r
                #print 'Task %d done with status %s' % ( r[0], r[1] )
         

    def _processResults( self, results ) :
        self._results.extend( results ) 
        

if __name__ == '__main__' :
    portGen = PortfolioGenerator( config.SP500, 7, 2 )
    driver = BackTestDriver( 'ANTICOR', portGen, xrange(4,6) )
    
    from IPython.parallel import Client
    #driver.run()
    driver.run_parallel( Client() )
    