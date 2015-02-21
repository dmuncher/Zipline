# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

"""Download and install Anaconda

Packages are downloaded/installed in parallel, allowing for faster installs
when using many nodes.

"""
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
from starcluster.utils import print_timing

import StringIO
import re


class AnacondaInstaller( DefaultClusterSetup ) :

    Installer_URL_2_1_0 = 'http://09c8d0b2229f813c1b93-c95ac804525aac4b6dba79b00b39d1d3.r79.cf1.rackcdn.com/Anaconda-2.1.0-Linux-x86_64.sh'

    INSTALL_DIR = '/usr/local/anaconda'
    BIN_DIR = INSTALL_DIR + '/bin'
    CONDA = BIN_DIR + '/conda'
    SYSTEM_ENV_FILE = '/etc/environment'
    PATH_pattern = re.compile( 'PATH=(.*)' )
    PATH_Pattern2 = re.compile( '"(.*)"' )

    def __init__( self, installer_url = None, packages = None ) :
        super( AnacondaInstaller, self ).__init__()
        self.url = installer_url or self.Installer_URL_2_1_0 
        self.packages = packages # Packages to install or update using anaconda
        if packages:
            self.packages = [ pkg.strip() for pkg in packages.split(',') ]


    def _log_and_execute( self, node, cmd ) :
        log.info( cmd )
        node.ssh.execute( cmd )


    def _update_system_env( self, node ) :
        envFile = node.ssh.remote_file( self.SYSTEM_ENV_FILE, 'r')
        envFileContents = envFile.read()
        envFile.close()

        newContents=''
        buf = StringIO.StringIO( envFileContents )
        for line in buf :
            m = self.PATH_pattern.match( line.rstrip() )
            if m is None :
                newContents += line
            else :
                path_value = m.group(1)
                m = self.PATH_Pattern2.match( path_value )
                if m is None :
                    newContents += ('PATH=%s:%s\n' % ( self.BIN_DIR, path_value ) )
                else :
                    newContents += ('PATH="%s:%s"\n' % ( self.BIN_DIR, m.group(1) ) )

        # Override /etc/environment file, prepend /usr/local/anaconda/bin to PATH
        envFile = node.ssh.remote_file( self.SYSTEM_ENV_FILE, 'w' )
        envFile.write( newContents )
        envFile.close()


    @print_timing( 'Update/install packages under Anaconda' )
    def install_packages( self, nodes, dest ) :
        log.info( 'Installing %s on %s:' % ( ' '.join( self.packages ), dest ) )

        commands = []
        for pkg in self.packages :
            if pkg == 'zipline' :
                commands.append( "echo -ne 'y' | %s install -c Quantopian zipline" % (self.CONDA) )
            else :
                commands.append( "echo -ne 'y' | %s install %s" % (self.CONDA, pkg) ) 
        for command in commands:
            log.info("$ " + command)
        cmd = "\n".join(commands)

        for node in nodes:
            self.pool.simple_job(node.ssh.execute, (cmd,), jobid=node.alias)
        self.pool.wait(len(nodes))


    @print_timing( 'AnacondaInstaller' )
    def install( self, user, nodes, dest='all nodes' ) :
        #if nodes[0].ssh.isfile( self.CONDA ) :
        #    log.info( 'Anaconda already installed' )
        #    #commands.append( '%s update conda; %s update anaconda' % ( self.CONDA, self.CONDA ) )
        #    return

        # Download and install Anaconda
        log.info( 'Installing Anaconda on %s:' % dest )
        update_system_env = nodes[0].ssh.isfile( self.SYSTEM_ENV_FILE )

        commands = []
        commands.append( 'wget -O Anaconda_Installer.sh %s; bash Anaconda_Installer.sh -b -f -p %s; rm Anaconda_Installer.sh' % ( self.url, self.INSTALL_DIR ) )
        if not update_system_env : 
            commands.append( "echo 'export PATH=%s:$PATH' >> ~root/.bashrc" % ( self.BIN_DIR ) )
        for command in commands:
            log.info("$ " + command)

        cmd = "\n".join(commands)
        for node in nodes:
            self.pool.simple_job( node.ssh.execute, (cmd,), jobid=node.alias )
        self.pool.wait( len(nodes) )

        # Update /etc/environment if present
        if update_system_env :
            log.info( 'Overriding %s' % ( self.SYSTEM_ENV_FILE ) )
            for node in nodes :
                #self.pool.simple_job(node.ssh.execute, (cmd,), jobid=node.alias)
                self.pool.simple_job( self._update_system_env, (node,), jobid=node.alias )
            self.pool.wait(len(nodes))
        else :
            # Only need to do this once since ~[user] is NFS mounted and visible to all nodes
            self._log_and_execute( nodes[0], "echo 'export PATH=%s:$PATH' >> ~%s/.bashrc" % ( self.BIN_DIR, user ) )

        # Update/install packages
        if self.packages :
            #log.info( 'conda: %s' % nodes[0].ssh.which( 'conda') )
            self.install_packages( nodes, dest )


    def run(self, nodes, master, user, user_shell, volumes):
        self.install( user, nodes )

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self.install( user, [node], dest=node.alias )

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        raise NotImplementedError("on_remove_node method not implemented")


            
        
