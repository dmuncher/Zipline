
import os, sys
import logging
#logging.basicConfig( filename = 'AlgoRunner_%s.log' % ( os.getpid() ),
#                     format = '[%(asctime)-15s] Runner: %(message)s', 
#                     level = logging.INFO )
LOGGER = logging.getLogger( __name__ )
fh = logging.FileHandler( 'AlgoRunner_%s.log' % ( os.getpid() ) )
fh.setFormatter( logging.Formatter( '[%(asctime)s] %(name)s - %(levelname)s: %(message)s',
                                    datefmt = '%Y-%m-%d %H:%M:%S' ) )
LOGGER.addHandler( fh )
LOGGER.setLevel( logging.INFO )

from collections import namedtuple

import zipline
from algos.algo_factory import create_algo


BackTestResult = namedtuple( 'BackTestResult', 'id, status' )

class AlgoRunner( object ):
    
    '''
    Each runner will handle multiple runs of the algorithm (different combinations of portfolios, parameters, startDate, endDate )
    and hold results for these runs
    '''
    
    
    def __init__ ( self, algoName, allStocks ) :
        self._algoName  = algoName
        self._allStocks = allStocks
        self._start     = None # start date
        self._end       = None # end date
        self._results   = []
        #print 'CWD:', os.getcwd()
        
    def run( self, task_id, startDate, endDate, port, *args ) :
        
        try :
            # Initialize algorithm
            algo = create_algo( self._algoName, port, *args )
        
            # Load data from yahoo for all stocks if (start, end) is different fromr previous run
            if startDate != self._start or endDate != self._end :
                LOGGER.info( 'Loading data: %s, %s' % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d')) )
                self._start, self._end = startDate, endDate
                self._data = zipline.data.load_from_yahoo( stocks=self._allStocks, indexes={}, start=self._start, end=self._end )
                self._data = self._data.dropna()
        
            # Run algorithm
            LOGGER.info( 'Running algo %s' % (self._algoName) )
            data = self._data.loc[:, port]   
            res = algo.run( data )
        
            # Record results
            LOGGER.info( 'Saving result #%d' % ( len(self._results) ) )
            self._results.append( ( port, args, self._start, self._end, res.portfolio_value ) )
            return BackTestResult( task_id, 'SUCCESS' )
        except Exception as e :
            return BackTestResult( task_id, str(e) )
    
        
    def results( self ) :
        return self._results