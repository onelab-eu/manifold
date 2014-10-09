#!/usr/bin/env python
# -*- coding: utf-8 -*-

#DEPRECATED|from __future__ import print_function
#DEPRECATED|
#DEPRECATED|def print(*args, **kwargs):
#DEPRECATED|    __builtins__.print("coucou")
#DEPRECATED|    __builtins__.print(*args, **kwargs)

import subprocess, operator

from twisted.internet                   import defer
from twisted.internet.task              import LoopingCall

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
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.util.reactor_thread       import ReactorThread

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


def async_sleep(secs):
    d = defer.Deferred()
    ReactorThread().callLater(secs, d.callback, None)
    return d

@defer.inlineCallbacks
def async_wait(fun, interval = 1):
    while not fun():
        yield async_sleep(interval)

class AgentDaemon(Daemon):

    DEFAULTS = {
        "server_mode"   : False,
    }

    ########################################################################### 
    # Supernode management
    ########################################################################### 

    @defer.inlineCallbacks
    def get_supernode(self, interface):
        
        # XXX supernodes = Supernode.get()
        # XXX Supernode should be attached to an interface... or at the the
        # router if we can route such requests.

        d = DeferredReceiver() # SyncReceiver
        interface.send(GET(),
                destination = Destination('supernode', namespace='local'),
                receiver = d)
        rv = yield d.get_deferred() # receiver.get_result_value().get_all()
        supernodes = rv.get_all()

        if not supernodes:
            defer.returnValue(None)

        # The agent might have previously registered as a supernode... avoid
        # loops to self.
        my_hostname = hostname()
        supernodes = [supernode['hostname'] for supernode in supernodes if supernode['hostname'] not in [my_hostname, SERVER_SUPERNODE]]

        if not supernodes:
            defer.returnValue(None)

        # BaseClass.set_options(deferred = True)
        # supernodes = yield SuperNode.collection(deferred=True)

        if not self._ping:
            # No ping tool available, choosing at random (or not :)
            defer.returnValue(supernodes[0])

        # Let's ping supernodes
        # XXX Such calls should be simplified
        d = DeferredReceiver() 
        self._ping.send(GET(), 
                destination = Destination('ping',
                    Filter().filter_by(Predicate('destination', 'included', supernodes)),
                    FieldNames(['destination', 'delay'])),
                receiver = d)
        rv = yield d.get_deferred() # receiver.get_result_value().get_all()
        delays = rv.get_all()

        # XXX ping: unknown host adreena
        # XXX This should triggered unregistration of a supernode

        # XXX syntax !
        # delays = yield Ping(destination in supernodes, fields=destination, # delay)
        # supernode = min(delays, key=operator.itemgetter('delay'))


        if not delays:
            defer.returnValue(None)

        # Let's keep the supernode of min delay (we have no rename since we are
        # not using the router Rename abilities
        supernode = min(delays, key=lambda d: float(d['probes'][0]['delay']))

        for delay in delays:
            Log.info("DELAY TO %s = %s" % (delay['destination'], delay['probes'][0]['delay']))
        Log.info("=> Selected %s" % (supernode['destination'],))

        defer.returnValue(supernode['hostname'] if supernode else None)

    @defer.inlineCallbacks
    def register_as_supernode(self, interface):
        # As a server, we would do this to create a new object locally.
        # Supernode(hostname = hostname()).insert()
        # We now want to create a new object remotely at the server
        # This should trigger an insert query directed towards the server
        d = DeferredReceiver()
        interface.send(CREATE(hostname = hostname()),
                destination = Destination('supernode', namespace='local'),
                receiver = d)
        rv = yield d.get_deferred()
        Log.info("Supernode registration done. Success ? %r" % (rv.get_all(),))

    def withdrawn_as_supernode(self, interface):
        pass


    @defer.inlineCallbacks
    def connect_interface(self, router, host):
        interface = router.add_interface('tcpclient', host)

        yield async_wait(lambda : interface.is_up())

        # XXX sleep until the interface is connected ?
        defer.returnValue(interface)

    @defer.inlineCallbacks
    def bootstrap_overlay(self, router):

        Log.info("Connecting to main server...")
        self._main_interface = yield self.connect_interface(router, SERVER_SUPERNODE)
        Log.info("Connected to main server. Interface=%s" % (self._main_interface,))

        Log.info("Getting supernodes...")
        supernode = yield self.get_supernode(self._main_interface) # XXX Blocking ???

        if supernode:
            Log.info("Connecting to supernode: %s..." % (supernode,))
            self._client_interface = yield self.connect_interface(router, supernode)
            Log.info("Connected to supernode: %s. Interface=%s" % (supernode, self._client_interface,))

        # Register as a supernode on the main server
        Log.info("Registering as supernode...")
        yield self.register_as_supernode(self._main_interface)


        if supernode:
            # Finally once we are all set up, disconnect the interface to the
            # server
            self._main_interface.down()

        router.get_fib().dump()

        # defer.returnValue()

    ########################################################################### 
    # Misc. unused
    ########################################################################### 

    def check_connectivity(self, interface):
        # XXX Can't we get events from the interface
        receiver = SyncReceiver()
        interface.send(PING(), Destination('object', namespace='local'), receiver = receiver)

        # How to detect timeout
        # What is a valid result
        ping_result = receiver.get_result_value().get_all()

    def on_interface_down(interface):
        """
        We lost the supernode/server
        We lost a client
        """
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

    ########################################################################### 
    # Main
    ########################################################################### 

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

        # XXX We need some auto-detection for processes
        #self._ping = router.add_platform("ping", "ping")
        self._ping = None

        # Setup interfaces
        #self._ws_interface  = router.add_interface('websocket')
        self._local_interface  = router.add_interface('unix')
        self._server_interface = router.add_interface('tcpserver') # Listener XXX port?

        # Setup peer overlay
        if Options().server_mode:
            announce, = Announces.from_string(SUPERNODE_CLASS)
            Supernode = ManifoldObject.from_announce(announce)
            
            Log.info("Bootstraping supernodes...")
            supernode_collection = ManifoldLocalCollection(Supernode)
            supernode_collection.insert(Supernode(hostname = hostname()))

            router.register_local_collection(supernode_collection)

            # XXX We should install a hook to remove from supernodes agents that have disconnected

        else:
            # The agent just builds the overlays and stays passive
            self.bootstrap_overlay(router)

            #self._bootstrap_overlay_task = LoopingCall(self.bootstrap_overlay, router)
            #self._bootstrap_overlay_task.start(0)

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
