#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# The OperatorGraph manages all the pending QueryPlan(s) running
# on a given Router.
#
# The OperatorGraph is made of Operator Node(s) exchanching
# Manifold Packets. This Graph is connected to the Manifold
# Sockets and Manifold Gateways and transport Manifold
# Packets (Query, Record...)
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

# We currently build on QueryPlan, the idea is in the end to merge the QueryPlan class in this class.
from manifold.core.packet       import Packet 
from manifold.core.query_plan   import QueryPlan
from manifold.core.node         import Node
from manifold.util.lattice      import Lattice
from manifold.util.log          import Log 

class OperatorGraph(object):
    """
    Replaces QueryPlan() and AST(), since operators are now mutualized.

    To begin with, the operator graph will be a set of parallel ASTs.
    """

    # Duplicated in manifold.core.interface
    LOCAL_NAMESPACE = "local"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router):
        """
        Constructor
        """

        # A pointer to the router to which the OperatorGraph belongs
        self._interface  = router
        
        # A lattice that maps the queries currently contained in the
        # OperatorGraph with the corresponding operators
        self._lattice = Lattice()


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def build_query_plan(self, packet):
        """
        Build the Query Plan according a QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        assert isinstance(packet, Packet) and packet.get_type() == Packet.TYPE_QUERY, \
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        query      = packet.get_query()
        annotation = packet.get_annotation()
        receiver   = packet.get_receiver()

        user = annotation.get('user', None)

        # Handling platforms
        namespace = None

        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table
            packet.set_query(query)

            if namespace == self.LOCAL_NAMESPACE:
                metadata = self._interface.get_local_metadata()
                allowed_platforms = list() 

            else: # namespace == 1 platform
                metadata = self._interface.get_metadata()
                allowed_platforms = [p['platform'] for p in self._interface.get_platforms() if p['platform'] == namespace]
        else:
            metadata = self._interface.get_metadata()
            allowed_platforms = [p['platform'] for p in self._interface.get_platforms()]

        # Handling metadata
        query_plan = QueryPlan(self._interface)
        query_plan.build(query, metadata, allowed_platforms, self._interface.allowed_capabilities, user)
        query_plan.dump()

        self._interface.init_from_nodes(query_plan, user)
        receiver.set_producer(query_plan.ast)
