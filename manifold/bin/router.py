#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Local router process reading on UNIX socket
#
# This file is part of the MANIFOLD project
#
# Copyright (C), UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

from manifold.core.router           import Router
from manifold.util.daemon           import Daemon
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.type             import accepts, returns

class RouterDaemon(Daemon):

    def __init__(self):
        """
        Constructor.
        """
        Daemon.__init__(
            self,
            self.terminate
        )

    def main(self):
        """
        Run ManifoldServer (called by Daemon::start).
        """
        Log.info("Starting RouterServer")

        self._router = Router()

        # Storage
        try:
            from manifold.storage import StorageGateway
            storage = StorageGateway(self._router)
            storage.set_up()
        except Exception, e:
            import traceback
            Log.warning(traceback.format_exc())
            Log.warning("Unable to load the Manifold Storage, continuing without storage")

        # Additional platforms
        self._router.add_interface("tcpserver")
        self._router.add_interface("test_timeout", "test_timeout") # XXX What if we don't provide a name here ?

        # XXX This is not used in agent, is it mandatory ?
        self.daemon_loop()

    def terminate(self):
        """
        Function called when the RouterDaemon must stops.
        """
        try:
            Log.info("Stopping gracefully RouterServer")
            self._router.terminate()
        except AttributeError:
            # self._router_server may not exists for instance if the
            # socket is already in use.
            pass

def main():
    Log.init_options()
    Daemon.init_options()
    Options().parse()
    RouterDaemon().start()

if __name__ == "__main__":
    main()
