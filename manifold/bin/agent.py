#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess

from manifold.core.router               import Router
from manifold.interfaces.tcp_socket     import TCPSocketInterface
from manifold.interfaces.unix_socket    import UNIXSocketInterface
from manifold.util.daemon               import Daemon
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

class AgentDaemon(Daemon):

    DEFAULTS = {
        "server_mode"   : False,
    }

    def __init__(self):
        Daemon.__init__(self, self.terminate)

    def get_supernode(self):
        return 'dryad.ipv6.lip6.fr'

    def make_agent_router(self):
        router = Router()

        # XXX We need some auto-detection for processes
        print "Adding platform ping"
        router.add_platform("ping", "ping_process")

        return router

    @staticmethod
    def init_options():
        """
        Prepare options supported by RouterDaemon.
        """
        options = Options()

        options.add_argument(
            "-s", "--server-mode", dest = "server_mode",
            help = "Socket that will read the Manifold router.",
            default = AgentDaemon.DEFAULTS["socket_path"]
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
            # Connect to supernode
            # a) get supernode...
            supernode = self.get_supernode()
            # b) connect...
            self._client_interface = TCPSocketInterface(self._router)
            self._client_interface.connect(supernode)
        else:
            self._client_interface = None

        self.daemon_loop()

    def terminate(self):
        self._local_interface.terminate()
        self._server_interface.terminate()
        if self._client_interface:
            self._client_interface.terminate()
        
def main():
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
