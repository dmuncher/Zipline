
import datetime as dt
import random    
import itertools
from IPython.parallel.util import interactive
import sys
from collections import namedtuple

import backtest_harness.runner
import config
import utils.parallel
  
  
BackTestTask = namedtuple( 'BackTestTask', 'id, portfolio, params' ) 
               
class BackTestDriver( object ):
    
    def __init__( self, 
                  algo,               # name of algorithm
                  portfolios,         # different portflio compositions
                  paramSets,          # ranges of algorithm params to explore
                  nPeriods = 1,       # number of periods on which to run the algorithm
                  endDate = None,     # default is today 
                  periodLen = 365 ) : # default is 1Y 
        self._algo = algo
        self._paramSets = list( paramSets )
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

    def _generateTasks( self, initialTaskId = 0 ) :
        # Tag each task with task id
        for port, params in itertools.product( self._portfolios, self._paramSets ) :
            yield BackTestTask( initialTaskId, port, params )
            initialTaskId += 1
    
    
    def run( self ) :
        self._results = []
        
        # Initialize a single runner since we are running locally
        runner = backtest_harness.runner.AlgoRunner( self._algo, self._allStocks )
        
        for start, end in self._periods :
            #nTasks = len( self._portfolios ) * len( self._paramSets )
            #for task_id, port, params in itertools.izip( xrange( nTasks ),
            #                                             *zip( *itertools.product( self._portfolios, self._paramSets ) ) ) :
            for task in self._generateTasks() :    
                runner.run( task.id, start, end, task.portfolio, task.params )
                
        self._processResults( runner.getResults() )    
        
        
    def run_parallel( self, clusterClient ) :
        '''
        clusterClient could be either local or remote
        '''
        
        self._results = []
        
        dview = clusterClient[:]
        dview.clear()
        lview = clusterClient.load_balanced_view() 
        
        #@dview.remote(block = True)
        #def setupRunner( algoName, allStocks ) :
        #    from backtest_harness.runner import AlgoRunner
        #    runner = AlgoRunner( algoName, allStocks )
        #    return globals().keys()
        #print setupRunner( self._algo, self._allStocks )
        #sys.exit(1)
        
        # Import runner module and initialize a runner on each node
        utils.parallel.import_and_reload( dview, 'backtest_harness.driver', alias = None, components = ['BackTestTask'] )
        utils.parallel.import_and_reload( dview, 'backtest_harness.runner' )
        cmd = "runner = backtest_harness.runner.AlgoRunner('%s', %s)"
        dview.execute( cmd % (self._algo, self._allStocks), block = True )
        
        remote_f1 = interactive( lambda task : runner.run( task.id, startDate, endDate, task.portfolio, task.params ) )
        
        @dview.remote( block = True )
        def remote_f2() :
            return runner.getResults()
        
        for start, end in self._periods :
            dview['startDate'], dview['endDate'] = start, end
            
            # Non-blocking gather
            amr = lview.map_async( remote_f1,
                                  self._generateTasks(),
                                  ordered = False ) # Get results first-come-first, don't care about the order
            
            # Wait for results and process them as they arrive
            #amr.wait_interactive()
            #clusterClient.spin()
            #amr.display_outputs( groupby = 'type' )
            for taskResult in amr :
                print 'Task %d done with status %s' % ( taskResult.id, taskResult.status )
         
            # Collect results
            for runnerResults in remote_f2() :
                self._processResults( runnerResults ) 
            

    def _processResults( self, results ) :
        self._results.extend( results ) 
        print 'accumulate results', len( self._results )
        

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
        
        
if __name__ == '__main__' :
    portGen = PortfolioGenerator( config.SP500, 7, 2 )
    driver = BackTestDriver( 'ANTICOR', portGen, xrange(4,8) )
    
    from IPython.parallel import Client
    #driver.run()
    driver.run_parallel( Client() )
    