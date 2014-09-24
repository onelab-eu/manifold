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

    def __init__(self):
        Daemon.__init__(self, self.terminate)

    def get_supernode(self):
        
        # XXX supernodes = Supernode.get()
        # XXX Supernode should be attached to an interface... or at the the
        # router if we can route such requests.

        receiver = SyncReceiver()
        print "Requesting supernodes to the server"
        self._client_interface.send(GET(), Destination('local:supernode'), receiver = receiver)

        supernodes = receiver.get_result_value().get_all()

        print "SUPERNODES=", supernodes
        return supernodes[0] if supernodes else None

    def make_agent_router(self):
        router = Router()

        # XXX We need some auto-detection for processes
        router.add_platform("ping", "ping_process")

        # Register local objects
        router.register_object(Supernode, 'local')

        return router

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
        self._router = self.make_agent_router()

        # Create local interface
        self._local_interface = UNIXSocketInterface(self._router)

        # Create appropriate interfaces to listen to queries
        self._server_interface = TCPSocketInterface(self._router)
        self._server_interface.listen()

        if not Options().server_mode:
            # a) Connect to main server
            # XXX An interface should connect to a single remote host
            self._client_interface = TCPSocketInterface(self._router).connect('dryad.ipv6.lip6.fr')
            # Connect to supernode
            # b) get supernode...
            supernode = self.get_supernode()
            self._client_interface.disconnect()
            # c) connect...
            self._client_interface = TCPSocketInterface(self._router).connect(supernode)
        else:
            # The current agent registers itself as a supernode
            # XXX import supernode
            s = Supernode(hostname = hostname())
            s.insert()
            self._client_interface = None

        #self.daemon_loop()

    def terminate(self):
        self._local_interface.terminate()
        self._server_interface.terminate()
        if self._client_interface:
            self._client_interface.terminate()
        
def main():
    AgentDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
