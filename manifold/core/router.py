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

import errno, os, traceback

from manifold.core.capabilities     import Capabilities
from manifold.core.code             import BADARGS, ERROR
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.interface        import Interface
from manifold.core.key              import Keys
from manifold.core.method           import Method
from manifold.core.operator_graph   import OperatorGraph
from manifold.core.packet           import ErrorPacket, Packet
from manifold.core.result_value     import ResultValue
from manifold.core.socket           import Socket
from manifold.policy                import Policy
from manifold.util.filesystem       import mkdir
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.storage          import STORAGE_NAMESPACE
from manifold.util.type             import returns, accepts

# XXX cannot use the wrapper with sample script
# XXX cannot use the thread with xmlrpc -n
#from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

CACHE_LIFETIME = 1800
VAR_DIR        = "/var/lib/manifold"

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
# Router class is an Interface: 
# builds the query plan, and execute query plan using deferred if required
#------------------------------------------------------------------

# TODO remove Interface inheritance
class Router(Interface):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, user_storage = None, allowed_capabilities = None, var_dir = VAR_DIR):
        """
        Constructor.
        Args:
            user_storage: A dictionnary used to access to the Manifold Storage
                or None if the Storage can be accessed anonymously.
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Router. Pass None if there
                is no restriction.
            var_dir: An absolute path corresponding to a folder in which this
                Router can store data.
        """
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so
        # that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self, user_storage, allowed_capabilities)

        try:
            mkdir(var_dir)
        except OSError, e:
            Log.warning(e)

        self._sockets = list()

        # Manifold Gateways are already initialized in parent class. 
        self._operator_graph = OperatorGraph(router = self)

        # XXX metadata/g_3nf

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(DBGraph)
    def get_dbgraph(self):
        """
        Returns:
            The DBGraph related to all the Tables except those
            provided by the Manifold Storage.
        """
        return self.g_3nf

    @returns(DBGraph)
    def get_dbgraph_storage(self):
        """
        Returns:
            The DBGraph related to the Manifold Storage. 
        """
        # We do not need normalization here, can directly query the Gateway

        # 1) Fetch the Storage's announces and get the corresponding Tables.
        local_announces = self._storage.get_announces()
        local_tables = [announce.get_table() for announce in local_announces]

        # 2) Build the corresponding map of Capabilities
        map_method_capabilities = dict()
        for announce in local_announces:
            table = announce.get_table()
            table_name = table.get_name()
            method = Method(STORAGE_NAMESPACE, table_name)
            capabilities = table.get_capabilities()
            map_method_capabilities[method] = capabilities 

        # 3) Build the corresponding DBGraph
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

    def __exit__(self, type = None, value = None, traceback = None):
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
        assert isinstance(packet, Packet),\
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        # Create a Socket holding the connection information and bind it.
        socket = Socket(consumer = packet.get_receiver())
        packet.set_receiver(socket)

        # Build the AST and retrieve the corresponding root_node Operator instance.
        query = packet.get_query()
        annotation = packet.get_annotation()

        try:
            root_node = self._operator_graph.build_query_plan(query, annotation)
            if not root_node:
                raise RuntimeError("Invalid root_node")

            root_node.add_consumer(socket)
            socket.receive(packet)
        except Exception, e:
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS, 
                message   = "Unable to build a suitable Query Plan (query = %s): %s" % (query, e),
                traceback = traceback.format_exc()
            )
            socket.receive(error_packet)
