
import sys
import logging
logging.basicConfig( format = '%(asctime)-15s %(message)s', level = logging.DEBUG, stream = sys.stdout )
LOGGER = logging.getLogger( __name__ )

import zipline
from algos.algo_factory import create_algo


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
        
        
    def run( self, startDate, endDate, port, *args ) :
        
        # Initialize algorithm
        algo = create_algo( self._algoName, port, *args )
        
        # Load data from yahoo for all stocks if (start, end) is different fromr previous run
        if startDate != self._start or endDate != self._end :
            LOGGER.debug( 'Loading data: %s, %s' % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d')) )
            self._start, self._end = startDate, endDate
            self._data = zipline.data.load_from_yahoo( stocks=self._allStocks, indexes={}, start=self._start, end=self._end )
            self._data = self._data.dropna()
        
        # Run algorithm
        data = self._data.loc[:, port]   
        res = algo.run( data )
        
        # Record results
        self._results.append( ( port, args, self._start, self._end, res.portfolio_value ) )
        LOGGER.debug( 'Getting result %d' % ( len(self._results) ) )
        
        
    def results( self ) :
        return self._results