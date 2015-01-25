#!/usr/bin/env python
# -*- coding: utf-8 -*-

#DEPRECATED|from __future__ import print_function
#DEPRECATED|
#DEPRECATED|def print(*args, **kwargs):
#DEPRECATED|    __builtins__.print("coucou")
#DEPRECATED|    __builtins__.print(*args, **kwargs)

# XXX Multiple times registered as supernode
# XXX Not reconnected when main node disconnect
# XXX supernode = None even when reconnected

import subprocess, operator, time

from twisted.internet                   import defer
from twisted.internet.task              import LoopingCall

from manifold.core.announce             import Announces
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.core.destination          import Destination
from manifold.core.field_names          import FieldNames
from manifold.core.filter               import Filter
from manifold.core.object               import Object
from manifold.core.packet               import GET, CREATE
from manifold.core.router               import Router
from manifold.gateways.object           import ManifoldLocalCollection
from manifold.util.daemon               import Daemon
from manifold.util.filesystem           import hostname
from manifold.util.log                  import Log
from manifold.util.async                import async_sleep, async_wait
from manifold.util.options              import Options
from manifold.util.predicate            import Predicate
from manifold.util.reactor_thread       import ReactorThread

SERVER_SUPERNODE = 'dryad.ipv6.lip6.fr'

SUPERNODE_CLASS = """
class supernode {
    string hostname;
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

class AgentDaemon(Daemon):

    DEFAULTS = {
        "server_mode"   : False,
    }

    ########################################################################### 
    # Supernode management
    ########################################################################### 

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

    def _up_callback(self, interface):
        Log.warning("Interface is up.")

    @defer.inlineCallbacks
    def _down_callback(self, interface):
        Log.warning("Overlay disconnected.")
        self._router.set_keyvalue('agent_supernode_state', 'down')
        self._router.set_keyvalue('agent_supernode_started', time.time())

        self.connect_to_supernode()

    @defer.inlineCallbacks
    def connect_interface(self, host):
        interface = self._router.add_interface('tcpclient', host=host, up = False)
        yield self._connect_interface(interface)
        defer.returnValue(interface)

    @defer.inlineCallbacks
    def connect_interface_until_success(self, host):
        interface = self._router.add_interface('tcpclient', host=host, up = False)
        yield self.reconnect_interface(interface)
        defer.returnValue(interface)

    @defer.inlineCallbacks
    def _connect_interface(self, interface):

        interface.up()
        Log.warning("Missing error handling: timeout, max_clients, rtt check, etc.")

        # XXX We have a callback when the interface is up, can we use it ?
        yield async_wait(lambda : interface.is_up() or interface.is_error())

        if interface.is_up():
            self._banned_supernodes = list()
            defer.returnValue(interface)
        else:
            # Error... This is where should should take proper action on
            # non-working supernodes: unregistration, etc.
            self._banned_supernodes.append(host)
            defer.returnValue(None)

        # XXX sleep until the interface is connected ?

    @defer.inlineCallbacks
    def reconnect_interface(self, interface):
        # This function should only terminate when the interface is reconnected
        # for sure

        Log.info("Trying to reconnect interface %r" % (interface,))
        interface.unset_error()
        interface.reconnect()
        yield async_wait(lambda : interface.is_up() or interface.is_error())

        if interface.is_error():
            self._router.set_keyvalue('agent_supernode_state', 'error')
            Log.info("Error connecting... Waiting 10s before re-attempting reconnection for interface %r" % (interface,))
            yield async_sleep(10)
            self.reconnect_interface(interface)
        else:
            Log.info("Interfce %r is up again" % (interface,))
            self._router.set_keyvalue('agent_supernode_state', 'up')

        defer.returnValue(None)
        
    @defer.inlineCallbacks
    def get_supernodes_from_interface(self, interface):
        """
        Returns:
            a list of hostnames
        """
        # XXX supernodes = Supernode.get()
        # XXX Supernode should be attached to an interface... or at the the
        # router if we can route such requests.

        # BaseClass.set_options(deferred = True)
        # supernodes = yield SuperNode.collection(deferred=True)


    @defer.inlineCallbacks
    def get_supernode_delays(self, supernodes):
        """
        Parameters:
            supernodes : a list of hostnames

        Returns:
            a dict { hostname : delay }
        """

        if not self._ping:
            # No ping tool available, or a single supernode, we pick the first
            # one (what about choosing at random?)
            defer.returnValue(None)

        # Let's ping supernodes
        # XXX Such calls should be simplified
        # XXX We send a single probe...
        d = DeferredReceiver() 
        self._ping.send(GET(), 
                destination = Destination('ping',
                    Filter().filter_by(Predicate('destination', 'included', list(supernodes))),
                    FieldNames(['destination', 'delay'])),
                receiver = d)
        rv = yield d.get_deferred() # receiver.get_result_value().get_all()
        rv_delays = rv.get_all()

        delays = dict( [(x['destination'], x['probes'][0]['delay']) for x in rv_delays] )

        # XXX ping: unknown host adreena
        # XXX This should triggered unregistration of a supernode

        # XXX syntax !
        # delays = yield Ping(destination in supernodes, fields=destination, # delay)
        # supernode = min(delays, key=operator.itemgetter('delay'))

        defer.returnValue(delays)

    @defer.inlineCallbacks
    def get_best_supernode(self, supernodes):
        if len(supernodes) > 1:
            # Try to get delays
            delays = yield self.get_supernode_delays(supernodes)
            if not delays:
                # Get the first of the set
                first = iter(supernodes).next()
                defer.returnValue(first)

            # Let's keep the supernode of min delay (we have no rename since we are
            # not using the router Rename abilities
            best_supernode = None
            best_delay     = None
            for supernode in supernodes:
                Log.info("DELAY TO %s = %s" % (supernode, delays[supernode]))
                if not best_supernode or delays[supernode] < best_delay:
                    best_supernode = supernode
                    best_delay = delays[supernode]

            Log.info("=> Selected %s" % (best_supernode,))
            defer.returnValue(best_supernode)
                    
        else:
            # Get the first of the set
            first = iter(supernodes).next()
            defer.returnValue(first)

    @defer.inlineCallbacks
    def get_supernodes(self):
        """
        Gets the full list of supernodes from the main server (except the main server itself).

        Returns:
            a set of hostnames
        """
        # Either connect to the main server, or tap into cache
        # XXX Need to be sure they have not all been blacklisted
        # XXX We could pre-sort them by ping
        Log.info("Connecting to main server...")
        Log.warning("We should avoid exchanging routes")
        interface = yield self.connect_interface_until_success(SERVER_SUPERNODE)

        Log.info("Getting supernodes from main server...")
        d = DeferredReceiver() # SyncReceiver
        interface.send(GET(),
                destination = Destination('supernode', namespace='tdmi'),
                receiver = d)
        rv = yield d.get_deferred() # receiver.get_result_value().get_all()
        received_supernodes = rv.get_all()

        interface.down()

        supernodes = set()
        if received_supernodes:
            # Filter out spurious supernodes (eg. the agent might have
            # previously registered as a supernode... avoid loops to self.)
            my_host = hostname()
            for supernode in received_supernodes:
                host = supernode.get('hostname', None)
                if not host:
                    continue
                if host in [my_host, SERVER_SUPERNODE]:
                    continue
                supernodes.add(host)

        defer.returnValue(supernodes)

    @defer.inlineCallbacks
    def connect_to_supernode(self):

        # Getting a set of supernode hostnames (eventually contacting the main server)
        supernodes = yield self.get_supernodes()
        if supernodes:
            supernode = yield self.get_best_supernode(supernodes)
        else:
            supernode = SERVER_SUPERNODE

        #self._router.set_keyvalue('agent_supernode', SERVER_SUPERNODE)
        #self._router.set_keyvalue('agent_supernode_state', 'up')
        #self._router.set_keyvalue('agent_supernode_started', time.time())

        Log.info("Connecting to supernode: %s..." % (supernode,))
        interface = yield self.connect_interface(supernode)
        if not interface:

            if supernode == SERVER_SUPERNODE:
                # Could not connect to main server as supernode... wait 10s and try the whole process again
                Log.info("Could not connect to main server as supernode... Reattempting in 10s")
                yield async_sleep(10)
                yield self.connect_to_supernode()

            else:
                Log.info("Could not connect to supernode '%s'. Trying another one." % (supernode,))
                self._supernodes.remove(supernode)
                yield self.connect_to_supernode()
            defer.returnValue(None)
                
        Log.info("Connected to supernode: %s. Interface=%s" % (supernode, interface,))
        self._router.set_keyvalue('agent_supernode', supernode)
        self._router.set_keyvalue('agent_supernode_state', 'up')
        self._router.set_keyvalue('agent_supernode_started', time.time())
        interface.add_down_callback(self._down_callback)

        # Register as a supernode on the main server
        Log.info("Registering as supernode...")
        yield self.register_as_supernode()

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

        self._router.set_keyvalue('agent_started', time.time())

        # XXX We need some auto-detection for processes
        self._ping = self._router.add_interface("ping", name="ping")
        self._paristraceroute = self._router.add_interface("paristraceroute", name="paristraceroute")
        if not Options().server_mode:
            # Is it because of a memory leak ?
            # Should we run fastping as a process instead of a thread ?
            self._fastping = self._router.add_interface("fastping", name="fastping")

        # Setup interfaces
        self._ws_interface  = self._router.add_interface('websocketserver')
        self._local_interface  = self._router.add_interface('unixserver')
        self._server_interface = self._router.add_interface('tcpserver') # Listener XXX port?

        Log.info("Bootstraping supernodes...")
        supernode_collection = ManifoldLocalCollection(Supernode)
        self._router.register_collection(supernode_collection, namespace='tdmi')
        self._supernode_collection = supernode_collection

        # Setup peer overlay
        if Options().server_mode:

            self._router.add_interface("dns", name="dns")
            self._router.add_interface("csv", name="nodes", **NODES_CSV_CONFIG)
            #self._router.add_interface("airports", "csv", AIRPORTS_CSV_CONFIG)
            #self._router.add_interface("tdmi", name="tdmi") # XXX ? clitos ?

            self.register_as_supernode() 


        else:
            # The agent just builds the overlays and stays passive
            self._banned_supernodes = list()
            self.connect_to_supernode()

            #self._bootstrap_overlay_task = LoopingCall(self.bootstrap_overlay, router)
            #self._bootstrap_overlay_task.start(0)

        # XXX We need to periodically check for connectivity

        #self._router.get_fib().dump()
        
def main():
    AgentDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()

    AgentDaemon().start()

if __name__ == "__main__":
    main()
