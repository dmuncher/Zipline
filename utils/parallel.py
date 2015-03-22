
def import_and_reload( dview, moduleName, alias = None, components = None ) :
    if alias is None :
        cmd = 'import %s; reload(%s)' % ( moduleName, moduleName )
    else :
        cmd = 'import %s as %s; reload(%s)' % (moduleName, alias, alias )
    if components :
        for c in components :
            cmd += '; %s = %s.%s' % (c, moduleName, c)
    #print 'cmd:', cmd
    dview.execute( cmd, block = True )
    