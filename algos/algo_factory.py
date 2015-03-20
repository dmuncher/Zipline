

def my_import( name ) :
    #print 'importing', name
    m = __import__( name )
    
    # __import__ only returns top level module
    # If name is a package, need to traverse down to retrieve bottom level module
    for n in name.split( '.' )[1:] :
        m = getattr( m, n )
    return m

def create_algo( algoName, *args, **kwargs ):
    print 'args', args
    package = create_algo.algoName2Package.get( algoName, 'Not found' )
    if package == 'Not found' :
        raise RuntimeError( 'Algorithm %s not registered' % (algoName) )

    module = my_import( package )
    algo_class = getattr( module, algoName )
    algo = algo_class( *args, **kwargs )
    return algo   
create_algo.algoName2Package = {
    'ANTICOR': 'algos.anticor'
}
