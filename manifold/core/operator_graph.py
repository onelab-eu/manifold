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
from manifold.core.destination      import Destination
from manifold.core.node             import Node
from manifold.core.query_plan       import QueryPlan
from manifold.util.lattice          import Lattice
from manifold.util.log              import Log
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
            router: A Router instance (needed to access to internal DBGraphs).
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

    #@returns(Node)
    def build_query_plan(self, destination, annotation, exclude_interfaces = None):
        """
        Build the Query Plan according to a Query and its optionnal Annotation.
        Args:
            query: A Query instance.
            annotation: An Annotation instance.
        Raises:
            Exception: if the QueryPlan cannot be built.
        Returns:
            The Node corresponding to the root node of the QueryPlan (most
            of time this is the top Operator of the AST).
        """
        # Check parameters
        assert isinstance(destination, Destination),\
            "Invalid destination %s (%s)" % (destination, type(destination))
        assert isinstance(annotation, Annotation),\
            "Invalid annotation %s (%s)" % (annotation, type(annotation))

        # Retrieve the DBGraph to compute a QueryPlan in respect of
        # namespace explicitly set in the Query and user's grants.
        user      = annotation.get("user", None)

        # Build the QueryPlan according to this DBGraph and to the user's Query.
        # and return the corresponding Node (if any)
        query_plan = QueryPlan()

        # allowed_platforms can be either an empty_list, meaning all, or a list
        # of strings
        return query_plan.build(destination, self.get_router(), set(), user, exclude_interfaces = exclude_interfaces)
