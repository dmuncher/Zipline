
from IPython.parallel import Client

# Create client to remote cluster with a json file that describe the cluster and
# your private ssh key
rc=Client('C:\Users\Bolei\Zipline\AWS\SecurityGroup@sc-zipline-us-east-1.json',sshkey='C:\Users\Bolei\Zipline\AWS\ec2-key-pair-us-east-1.rsa')

# Create a DirectView over the client
dview=rc[:]

# Check your python path is properly set
@dview.remote(block=True)
def setup() :
    import sys
    return sys.path

setup()

# Append path to your own code if needed
@dview.remote(block=True)
def setup( p ) :
    import sys
    sys.path=path
    sys.path.append( p )

setup(p)

# Import your module into remote python process's global namespace
dview.execute( 'import anticor', block=True )

# Send python objects to remote process
import datetime
start = datetime.datetime(2014, 1, 15)
end = datetime.datetime(2015, 1, 15)
dview['start'] = start
dview['end'] = end

# Execute parallel jobs
dview.map_sync( lambda w: anticor.run( start, end, w ), xrange(4, 12) )
