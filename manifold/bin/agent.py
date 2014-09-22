#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess

from manifold.core.router           import Router
from manifold.core.interface        import Interface
from manifold.util.daemon           import Daemon
from manifold.util.log              import Log
from manifold.util.options          import Options

# An agent is a router preconfigured with all found measurement tools + an
# XMLRPC/other remotely accessible interface
# ...or...
# An agent is responsible for runnning processes (that will be done later)
#   p = subprocess.Popen(ROUTER, )
#   self._processes['router'] = p
#   print p.pid
#   p.wait()

class AgentDaemon(Daemon):

    def get_supernode(self):
        return 'dryad.ipv6.lip6.fr'

    def make_agent_router(self):
        router = Router()

        # XXX We need some auto-detection for processes
        router.add_platform("ping", "ping_process")

        return router


    def main(self):
        # Create a router instance
        self._router = self.make_agent_router()

        # Create appropriate interfaces to listen to queries
        self._server_interface = Interface(router = self)
        self._server_interface.listen()

        # Connect to supernode
        # a) get supernode...
        supernode = self.get_supernode()
        # b) connect...
        self._client_interface = Interface(self._router)
        self._client_interface.connect(supernode)

        self.daemon_loop()
        
def main():
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
