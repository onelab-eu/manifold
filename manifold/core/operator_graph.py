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
from manifold.core.annotation       import Annotation
from manifold.core.query            import Query
from manifold.core.query_plan       import QueryPlan
from manifold.core.producer         import Producer 
from manifold.util.lattice          import Lattice
from manifold.util.log              import Log 
from manifold.util.storage          import STORAGE_NAMESPACE
from manifold.util.type             import accepts, returns 

class OperatorGraph(object):
    """
    Replaces QueryPlan() and AST(), since operators are now mutualized.

    To begin with, the operator graph will be a set of parallel ASTs.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router):
        """
        Constructor.
        Args:
            router: A Interface instance.
        """

        # A pointer to the router to which the OperatorGraph belongs
        self._router = router
        
        # A lattice that maps the queries currently contained in the
        # OperatorGraph with the corresponding operators
        self._lattice = Lattice()


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #@returns(Router)
    def get_router(self):
        return self._router

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Producer)
    def build_query_plan(self, query, annotation):
        """
        Build the Query Plan according to a Query and its optionnal Annotation.
        Args:
            query: A Query instance.
            annotation: An Annotation instance.
        Returns:
            The Producer corresponding to the root node of the QueryPlan (most
            of time this is the top Operator of the AST). 
        """
        # Check parameters
        assert isinstance(query, Query),\
            "Invalid query %s (%s)" % (query, type(query))
        assert isinstance(annotation, Annotation),\
            "Invalid annotation %s (%s)" % (annotation, type(annotation))

        # Retrieve the DBGraph to compute a QueryPlan in respect of
        # namespace explicitly set in the Query and user's grants. 
        router = self.get_router()
        user   = annotation.get('user', None)
        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table

            if namespace == STORAGE_NAMESPACE:
                db_graph = router.get_local_metadata()
                allowed_platforms = list() 
            else: # namespace == 1 platform
                db_graph = router.get_metadata()
                allowed_platforms = [p['platform'] for p in router.get_platforms() if p['platform'] == namespace]
        else:
            db_graph = router.get_metadata()
            allowed_platforms = [p['platform'] for p in router.get_platforms()]

        # Build the QueryPlan according to this DBGraph and to the user's Query. 
        query_plan = QueryPlan(router)
        query_plan.build(query, db_graph, allowed_platforms, router.get_capabilities(), user)
        query_plan.dump()

        # Return the corresponding Producer (if any)
        producer = query_plan.ast
        assert isinstance(producer, Producer), "Invalid producer = %s (%s)" % (producer, type(producer))
        return producer 

#DEPRECATED|        self._interface.init_from_nodes(query_plan, user)
