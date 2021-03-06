
import pandas as pd
import collections

# Config root logger to print to stdout
import logging
sh = logging.StreamHandler()
sh.setFormatter( logging.Formatter( '[%(asctime)s] %(name)s - %(levelname)s: %(message)s',
                                    datefmt = '%Y-%m-%d %H:%M:%S' ) )
rootLogger = logging.getLogger()
rootLogger.addHandler( sh )


class IndexCollection( collections.Mapping ) :
    
    def __init__( self, indexInfo ) :
        self._indexInfo = indexInfo
        self._d = dict( (k, None) for k in self._indexInfo.iterkeys() )
    
    def __iter__( self ) :
        return iter( self._d )

    def __len__( self ) :
        return len( self._d )
        
    def __getitem__( self, indexName ) :
        df = self._d[ indexName ]
        if df is None :
            df = pd.read_csv( self._indexInfo[ indexName ] )
            df.set_index( 'Symbol', inplace = True )
            self._d[ indexName ] = df
        return df


SP500 = 'SP500'
StockIndices = IndexCollection( 
    { SP500 : '/home/bolei/workspace/Zipline/data/SP500.csv' } )

