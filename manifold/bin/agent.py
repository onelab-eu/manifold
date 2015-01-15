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
from manifold.core.object               import Object
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
from manifold.gateways.object           import ManifoldLocalCollection
from manifold.core.packet               import GET, CREATE
from manifold.core.query                import Query
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.util.reactor_thread       import ReactorThread

SERVER_SUPERNODE = 'dryad.ipv6.lip6.fr'

SUPERNODE_CLASS = """
class supernode {
    string hostname;
    double rtt;
    KEY(hostname);
};
"""
announce, = Announces.from_string(SUPERNODE_CLASS)
Supernode = Object.from_announce(announce)
NODES_CSV_CONFIG = {
    'hostname': {
        'filename': '/root/datasets/node-lat-lon.csv',
        'fields': [
            ['hostname', 'hostname'],
            ['latitude', 'double'],
            ['longitude', 'double'],
        ],
        'key': 'hostname',
    }
}

AIRPORTS_CSV_CONFIG = {
    'airports': {
        'filename': '/root/datasets/airports.csv',
        'fields': [
            ['iata_code', 'string'],
        ],
        'key': 'iata_code',
    }
}


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
                destination = Destination('supernode', namespace='tdmi'),
                receiver = d)
        rv = yield d.get_deferred() # receiver.get_result_value().get_all()
        received_supernodes = rv.get_all()

        if not received_supernodes:
            defer.returnValue(None)
        
        # The agent might have previously registered as a supernode... avoid
        # loops to self.
        my_host = hostname()
        supernodes = set()
        for supernode in received_supernodes:
            host = supernode['hostname'] 
            
            if host in [my_host, SERVER_SUPERNODE]:
                continue
            if host in self._banned_supernodes:
                continue
            supernodes.add(host)

        # Remove duplicates in supernodes. As it is the key, this should be enforced by the collection")
        supernodes = list(supernodes)

        print "SUPERNODES", supernodes

        if not supernodes:
            defer.returnValue(None)

        # BaseClass.set_options(deferred = True)
        # supernodes = yield SuperNode.collection(deferred=True)

        if not self._ping or len(supernodes) == 1:
            # No ping tool available, or a single supernode, we pick the first
            # one (what about choosing at random?)
            defer.returnValue(supernodes[0])

        # Let's ping supernodes
        # XXX Such calls should be simplified
        # XXX We send a single probe...
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

        print "DELAYS", delays

        if not delays:
            defer.returnValue(None)

        # Let's keep the supernode of min delay (we have no rename since we are
        # not using the router Rename abilities
        supernode = min(delays, key=lambda d: float(d['probes'][0]['delay']))

        for delay in delays:
            Log.info("DELAY TO %s = %s" % (delay['destination'], delay['probes'][0]['delay']))
        Log.info("=> Selected %s" % (supernode['destination'],))

        defer.returnValue(supernode['destination'] if supernode else None)

    #@defer.inlineCallbacks
    def register_as_supernode(self):
        sn = Supernode(hostname = hostname())
        self._supernode_collection.create(Supernode(hostname = hostname()))

        # XXX We should install a hook to remove from supernodes agents that have disconnected

        # Old version:
        #
        # As a server, we would do this to create a new object locally.
        # Supernode(hostname = hostname()).insert()
        # We now want to create a new object remotely at the server
        # This should trigger an insert query directed towards the server
        #d = DeferredReceiver()
        #interface.send(CREATE(hostname = hostname()),
        #        destination = Destination('supernode', namespace='tdmi'),
        #        receiver = d)
        #rv = yield d.get_deferred()
        #Log.info("Supernode registration done. Success ? %r" % (rv.get_all(),))

    def withdrawn_as_supernode(self, interface):
        pass

    @defer.inlineCallbacks
    def reconnect_interface(self, interface):
        # This function should only terminate when the interface is reconnected
        # for sure

        interface.unset_error()
        interface.reconnect()
        yield async_wait(lambda : interface.is_up() or interface.is_error())

        if interface.is_error():
            Log.info("Waiting 10s before attempting reconnection for interface %r" % (interface,))
            yield async_sleep(10)
            self.reconnect_interface(interface)

        defer.returnValue(None)
        
    def _up_callback(self, interface):
        interface_id = "main" if interface == self._main_interface else "client"
        Log.warning("Interface %s is up." % (interface_id,))

    @defer.inlineCallbacks
    def _down_callback(self, interface):
        if not self._main_interface:
            print "I: Ignored down callback since main interface is None"
            return

        if interface == self._main_interface:
            if self._reconnect_main:
                # If we did not requested the interface to go down...
                Log.warning("Main interface is down.")
                self.reconnect_interface(self._main_interface)
        else:
            Log.warning("Overlay disconnected.")
            # to get supernodes. note that we could keep some in cache, or
            # even connection to them.
            if self._main_interface.is_down():
                # We need to wait for the main interface to be up
                yield self.reconnect_interface(self._main_interface)
                # Old code:
                #self._main_interface.up()
                #yield async_wait(lambda : interface.is_up() or interface.is_error())

            # We do not yield since we expect this task to complete
            self.connect_to_supernode()

    @defer.inlineCallbacks
    def connect_interface(self, host):
        interface = self._router.add_interface('tcpclient', host=host)

        interface.add_down_callback(self._down_callback)

        Log.warning("Missing error handling: timeout, max_clients, rtt check, etc.")

        # XXX We have a callback when the interface is up, can we use it ?
        yield async_wait(lambda : interface.is_up() or interface.is_error())

        if interface.is_up():
            print "Interface is up"
            self._banned_supernodes = list()
            defer.returnValue(interface)
        else:
            print "Interface is error"
            # Error... This is where should should take proper action on
            # non-working supernodes: unregistration, etc.
            self._banned_supernodes.append(host)
            defer.returnValue(None)

        # XXX sleep until the interface is connected ?

    @defer.inlineCallbacks
    def bootstrap_overlay(self):

        Log.info("Connecting to main server...")
        self._banned_supernodes = list()
        self._reconnect_main = True
        self._main_interface = yield self.connect_interface(SERVER_SUPERNODE)
        if not self._main_interface:
            Log.warning("Failed to connect main interface... try again later")
            Log.error("TODO")
            return
        Log.info("Connected to main server. Interface=%s" % (self._main_interface,))

        self.connect_to_supernode()

    @defer.inlineCallbacks
    def connect_to_supernode(self):

        Log.info("Getting supernodes...")
        supernode = yield self.get_supernode(self._main_interface) # XXX Blocking ???

        if supernode:
            Log.info("Connecting to supernode: %s..." % (supernode,))
            print "Sleeping 5s"
            import time
            time.sleep(5)
            self._client_interface = yield self.connect_interface(supernode)
            if not self._client_interface:
                Log.warning("Failed to connect to supernode. Trying again")
                Log.warning("We are requesting supernodes again... not very efficient")
                Log.warning("We should fallback on periodically trying to contact the main server in the worst case")
                
                self.connect_to_supernode()
                # This is ok since nobody should yield on self.connect_to_supernode()
                defer.returnValue(None)

            Log.info("Connected to supernode: %s. Interface=%s" % (supernode, self._client_interface,))

        # Register as a supernode on the main server
        Log.info("Registering as supernode...")
        yield self.register_as_supernode()

        if supernode:
            # Finally once we are all set up, disconnect the interface to the
            # server
            self._reconnect_main = False
            Log.info("Disconnecting from main server")
            self._main_interface.down()

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
        self._router = Router()

        # XXX We need some auto-detection for processes
        self._ping = self._router.add_interface("ping", name="ping")
        self._fastping = self._router.add_interface("fastping", name="fastping")

        # Setup interfaces
        self._ws_interface  = self._router.add_interface('websocketserver')
        self._local_interface  = self._router.add_interface('unixserver')
        self._server_interface = self._router.add_interface('tcpserver') # Listener XXX port?

        Log.info("Bootstraping supernodes...")
        supernode_collection = ManifoldLocalCollection(Supernode)
        self._router.register_collection(supernode_collection, namespace='tdmi')
        self._supernode_collection = supernode_collection

        self._main_interface = None

        # Setup peer overlay
        if Options().server_mode:

            self._router.add_interface("dns", name="dns")
            self._router.add_interface("csv", name="nodes", **NODES_CSV_CONFIG)
            #self._router.add_interface("airports", "csv", AIRPORTS_CSV_CONFIG)

            self.register_as_supernode() 

            #self._router.get_fib().dump()

        else:
            # The agent just builds the overlays and stays passive
            self.bootstrap_overlay()

            #self._bootstrap_overlay_task = LoopingCall(self.bootstrap_overlay, router)
            #self._bootstrap_overlay_task.start(0)

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
