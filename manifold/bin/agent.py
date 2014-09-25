#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess

from manifold.core.destination          import Destination
from manifold.core.router               import Router
from manifold.interfaces.tcp_socket     import TCPSocketInterface
from manifold.interfaces.unix_socket    import UNIXSocketInterface
from manifold.util.daemon               import Daemon
from manifold.util.filesystem           import hostname
from manifold.util.log                  import Log
from manifold.util.options              import Options

# An agent is a router preconfigured with all found measurement tools + an
# XMLRPC/other remotely accessible interface
# ...or...
# An agent is responsible for runnning processes (that will be done later)
#   p = subprocess.Popen(ROUTER, )
#   self._processes['router'] = p
#   print p.pid
#   p.wait()

from manifold.core.annotation           import Annotation
from manifold.core.field                import Field
from manifold.core.key                  import Key
from manifold.core.local                import ManifoldObject
from manifold.core.packet               import GET
from manifold.core.query                import Query
from manifold.core.sync_receiver        import SyncReceiver
            
SERVER_SUPERNODE = 'dryad.ipv6.lip6.fr'

# In memory object. Could be sqlite.
# Capabilities ?
class Supernode(ManifoldObject):

    __object_name__ = 'supernode'
    __fields__ = [
        Field('string', 'hostname'),
        Field('float',  'rtt'),
    ]
    __keys__ = [
        Key([f for f in __fields__ if f.get_name() == 'hostname']),
    ]

class AgentDaemon(Daemon):

    DEFAULTS = {
        "server_mode"   : False,
    }

    def get_supernode(self):
        
        # XXX supernodes = Supernode.get()
        # XXX Supernode should be attached to an interface... or at the the
        # router if we can route such requests.

        receiver = SyncReceiver()
        self._client_interface.send(GET(), destination=Destination('supernode', namespace='local'), receiver = receiver)
        supernodes = receiver.get_result_value().get_all()
        return supernodes[0]['hostname'] if supernodes else None

    @staticmethod
    def init_options():
        """
        Prepare options supported by RouterDaemon.
        """
        options = Options()

        options.add_argument(
            "-s", "--server-mode", dest = "server_mode", action='store_true',
            help = "Socket that will read the Manifold router.",
            default = AgentDaemon.DEFAULTS["server_mode"]
        )

    def main(self):
        # Create a router instance
        router = Router()

        self._local_interface  = router.add_interface('unix')
        self._server_interface = router.add_interface('tcpserver') # Listener XXX port?

        if not Options().server_mode:
            self._client_interface = router.add_interface('tcp', SERVER_SUPERNODE)
            supernode = self.get_supernode() # XXX Blocking ???
            #self._client_interface.down()
            self._client_interface.connect(supernode)

        else:
            # The current agent registers itself as a supernode
            # XXX import supernode
             Supernode(hostname = hostname()).insert()

        # XXX We need some auto-detection for processes
        router.add_platform("ping", "ping_process")

        # Register local objects
        router.register_object(Supernode, 'local')

        self._router = router

        #self.daemon_loop()
        
def main():
    AgentDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
