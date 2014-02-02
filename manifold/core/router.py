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

from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.interface        import Interface
from manifold.core.method           import Method
from manifold.core.operator_graph   import OperatorGraph
from manifold.util.log              import Log 
from manifold.util.storage          import STORAGE_NAMESPACE
from manifold.util.type             import returns, accepts

# XXX cannot use the wrapper with sample script
# XXX cannot use the thread with xmlrpc -n
#from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

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

    def __init__(self, allowed_capabilities = None):
        """
        Constructor.
        Args:
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Router. Pass None if there
                is no restriction.
        """
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so
        # that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self, allowed_capabilities)

#UNUSED|        self._sockets = list()

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
        local_announces = self.get_storage().get_gateway().get_announces()
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

    def update_platforms(self, platforms_enabled):
        super(Router, self).update_platforms(platforms_enabled)
        self.g_3nf = to_3nf(self.get_announces())

