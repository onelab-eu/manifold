#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold router handles Query, compute the corresponding QueryPlan,
# and deduce which Queries must be sent the appropriate Gateways.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.capabilities     import Capabilities
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.interface        import Interface
from manifold.core.key              import Keys
from manifold.core.method           import Method
from manifold.core.operator_graph   import OperatorGraph
from manifold.core.packet           import ErrorPacket, Packet 
from manifold.core.socket           import Socket
from manifold.policy                import Policy
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import returns, accepts

# XXX cannot use the wrapper with sample script
# XXX cannot use the thread with xmlrpc -n
#from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

CACHE_LIFETIME     = 1800

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
# Router class is an Interface: 
# builds the query plan, and execute query plan using deferred if required
#------------------------------------------------------------------

class Router(Interface):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, user_storage = None, allowed_capabilities = None):
        """
        Constructor.
        Args:
            user_storage: A dictionnary used to access to the Manifold Storage
                or None if the Storage can be accessed anonymously.
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Router. Pass None if there
                is no restriction.
        """
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so
        # that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self, user_storage, allowed_capabilities)

        self._sockets = list()

        # Manifold Gateways are already initialized in parent class. 
        self._operator_graph = OperatorGraph(router = self)

        # XXX metadata/g_3nf

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(DBGraph)
    def get_metadata(self):
        """
        Returns:
            The DBGraph related to all the Tables except those related to the
            "local:" namespace.
        """
        return self.g_3nf

    @returns(DBGraph)
    def get_local_metadata(self):
        """
        Returns:
            The DBGraph related to the "local:" namespace.
        """
        # We do not need normalization here, can directly query the gateway
        map_method_capabilities = {
            Method("local", "platform") : Capabilities("retrieve", "join", "selection", "projection"),
            Method("local", "object")   : Capabilities("retrieve", "join", "selection", "projection"),
            Method("local", "column")   : Capabilities("retrieve", "join", "selection", "projection")
        }
        local_announces = self._storage.get_announces()
        local_tables = [announce.get_table() for announce in local_announces]

        return DBGraph(local_tables, map_method_capabilities)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        self.cache = dict() 
        super(Router, self).boot()
        self.g_3nf = to_3nf(self.get_announces())

    def __enter__(self):
        """
        Function called back while entering a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().start_reactor()
        return self

    def __exit__(self, type=None, value=None, traceback=None):
        """
        Function called back while leaving a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().stop_reactor()

    def receive(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A QUERY Packet instance. 
        """
        assert isinstance(packet, Packet) and packet.get_type() == Packet.TYPE_QUERY, \
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        print "Router received a query", packet.get_query()
        # Create a Socket holding the connection information and bind it.
        print "Create socket..."
        socket = Socket(consumer = packet.get_receiver())
        print "... and set its receiver"
        packet.set_receiver(socket)

        # Build the AST and retrieve the corresponding root_node Operator instance.
        print "Build the AST"
        query = packet.get_query()
        annotation = packet.get_annotation()
        root_node = self._operator_graph.build_query_plan(query, annotation)

        # Execute the operators related to the socket, if needed.
        if root_node: 
            print "Root node", root_node, "got added the socket as a consumer", socket
            root_node.add_consumer(socket)
            print "=====> Socket should now have a producer", socket
            print "Socket is receiving packet", packet
            socket.receive(packet)
        else:
            socket.receive(ErrorPacket("Unable to build a suitable Query Plan (query = %s)" % query))
