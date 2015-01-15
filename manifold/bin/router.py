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
from manifold.util.constants        import STORAGE_DEFAULT_CONFIG, STORAGE_DEFAULT_GATEWAY
from manifold.util.daemon           import Daemon
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.type             import accepts, returns

class RouterDaemon(Daemon):
    DEFAULTS = {
        "storage_gateway" : STORAGE_DEFAULT_GATEWAY,
        "storage_config"  : STORAGE_DEFAULT_CONFIG
    }

    def __init__(self):
        """
        Constructor.
        """
        Daemon.__init__(
            self,
            self.terminate
        )

    @staticmethod
    def init_options():
        """
        Prepare options supported by RouterDaemon.
        """
        options = Options()

        options.add_argument(
            "-g", "--storage-gateway", dest = "storage_gateway",
            help = "The Manifold Gateway used to contact the Storage (usually %s)" % RouterDaemon.DEFAULTS["storage_gateway"],
            default = RouterDaemon.DEFAULTS["storage_gateway"]
        )

        options.add_argument(
            "-j", "--storage-config", dest = "storage_config",
            help = "(Requires --storage-gateway). Configuration passed to Manifold to contact the storage. Ex: %s" % RouterDaemon.DEFAULTS["storage_config"],
            default = RouterDaemon.DEFAULTS["storage_gateway"]
        )

    def main(self):
        """
        Run ManifoldServer (called by Daemon::start).
        """
        Log.info("Starting RouterServer")

        self._router = Router()
        try:
            from manifold.util.storage.storage import install_default_storage
            Log.warning("TODO: Configure a Storage in respect with Options(). Loading default Storage")
            install_default_storage(self._router)
        except Exception, e:
            import traceback
            Log.warning(traceback.format_exc())
            Log.warning("Unable to load the Manifold Storage, continuing without storage")

        self.daemon_loop()

    def terminate(self):
        """
        Function called when the RouterDaemon must stops.
        """
        try:
            Log.info("Stopping gracefully RouterServer")
            print "router terminate in terminate"
            self._router.terminate()
        except AttributeError:
            # self._router_server may not exists for instance if the
            # socket is already in use.
            pass

def main():
    RouterDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()
    RouterDaemon().start()

if __name__ == "__main__":
    main()
