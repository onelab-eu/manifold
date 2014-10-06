#!/usr/bin/env python
# -*- coding: utf-8 -*-

#DEPRECATED|from __future__ import print_function
#DEPRECATED|
#DEPRECATED|def print(*args, **kwargs):
#DEPRECATED|    __builtins__.print("coucou")
#DEPRECATED|    __builtins__.print(*args, **kwargs)

import subprocess, operator

from manifold.core.announce             import Announces
from manifold.core.destination          import Destination
from manifold.core.field_names          import FieldNames
from manifold.core.filter               import Filter
from manifold.core.router               import Router
from manifold.util.daemon               import Daemon
from manifold.util.filesystem           import hostname
from manifold.util.log                  import Log
from manifold.util.options              import Options
from manifold.util.predicate            import Predicate

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
from manifold.gateways.object           import ManifoldObject, ManifoldLocalCollection
from manifold.core.packet               import GET, CREATE
from manifold.core.query                import Query
from manifold.core.sync_receiver        import SyncReceiver

from gevent.threadpool import ThreadPool
            
SERVER_SUPERNODE = 'clitos.ipv6.lip6.fr'

SUPERNODE_CLASS = """
class supernode {
    string hostname;
    float rtt;
    KEY(hostname);
};
"""
#SERVER_SUPERNODE = 'dryad.ipv6.lip6.fr'

class AgentDaemon(Daemon):

    DEFAULTS = {
        "server_mode"   : False,
    }

    # @inlineCallbacks
    def get_supernode(self, interface):
        
        # XXX supernodes = Supernode.get()
        # XXX Supernode should be attached to an interface... or at the the
        # router if we can route such requests.

        receiver = SyncReceiver()
        interface.send(GET(),
                destination = Destination('supernode', namespace='local'),
                receiver = receiver)
        supernodes = receiver.get_result_value().get_all()

        # BaseClass.set_options(deferred = True)
        # supernodes = yield SuperNode.collection(deferred=True)

        # Let's ping supernodes
        # XXX Blocking
        # XXX Such calls should be simplified
        self._ping.send(GET(), 
                destination = Destination('ping',
                    Filter().filter_by(Predicate('destination', 'included', map(operator.itemgetter('hostname'), supernodes))),
                    FieldNames(['destination', 'delay']),
                    namespace='local'),
                receiver = receiver)
        delays = receiver.get_result_value().get_all()

        # XXX syntax !
        # delays = yield Ping(destination in supernodes, fields=destination, # delay)

        # Let's keep the supernode of min delay
        supernode = min(delays, key=operator.itemgetter('delay'))
        return supernode['hostname'] if supernode else None

    def register_as_supernode(self, interface):
        # As a server, we would do this to create a new object locally.
        # Supernode(hostname = hostname()).insert()
        # We now want to create a new object remotely at the server
        # This should trigger an insert query directed towards the server
        receiver = SyncReceiver()
        print("sending insert packet ofr supernode", hostname())
        print("interface", interface)
        interface.send(CREATE(hostname = hostname()),
                destination = Destination('supernode', namespace='local'),
                receiver = receiver)
        res = receiver.get_result_value().get_all()
        print("insert res", res)

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

    def check_connectivity(self, interface):
        # XXX Can't we get events from the interface
        receiver = SyncReceiver()
        interface.send(PING(), Destination('object', namespace='local'), receiver = receiver)

        # How to detect timeout
        # What is a valid result
        ping_result = receiver.get_result_value().get_all()

    def on_interface_down(interface):
        # Can we reconnect the interface ?
        if False:
            pass

        # Can we change supernode ?
        elif interface == self._client_interface:
            # Mark the former supernode as unreachable
            pass

            # Remove announces from FIB (?)
            # Is it optimal since we are connecting to an equivalent node
            pass

            # Get a new supernode
            supernode = self.get_supernode()
            self._client_interface.connect(supernode)



        

    def main(self):
        # Create a router instance
        router = Router()

        # XXX We need some auto-detection for processes
        self._ping = router.add_platform("ping", "ping")

        # Setup interfaces
        #self._ws_interface  = router.add_interface('websocket')
        self._local_interface  = router.add_interface('unix')
        self._server_interface = router.add_interface('tcpserver') # Listener XXX port?

        # Setup peer overlay
        if not Options().server_mode:
            self._main_interface = router.add_interface('tcp', SERVER_SUPERNODE)
            supernode = self.get_supernode(self._main_interface) # XXX Blocking ???
            #self._client_interface.down()

            ############self._client_interface = router.add_interface('tcp', supernode)
            #self._client_interface.connect(supernode)

            # Register as a supernode on the main server
            self.register_as_supernode(self._main_interface)

            # Finally once we are all set up, disconnect the connection to
            # server
            self._main_interface.down()
        else:

            announce, = Announces.from_string(SUPERNODE_CLASS)
            Supernode = ManifoldObject.from_announce(announce)
            
            supernode_collection = ManifoldLocalCollection(Supernode)
            supernode_collection.insert(Supernode(hostname = hostname()))

            router.register_local_collection(supernode_collection)

        #router.get_fib().dump()
        self._router = router

        # XXX We need to periodically check for connectivity

        #self.daemon_loop()
        
def main():
    AgentDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
