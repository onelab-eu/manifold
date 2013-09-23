#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Forwarder 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.result_value import ResultValue
from manifold.core.interface    import Interface
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
from manifold.core.router       import Router 

class Forwarder(Interface):

    # XXX This could be made more generic with the router
    # Forwarder class is an Interface 
    # builds the query plan, and execute query plan using deferred if required
    def forward(self, query, is_deferred = False, execute = True, user = None, receiver = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            is_deferred: A boolean set to True if this Query is async
            execute: A boolean set to True if the QueryPlan must be executed.
            user: The user issuing the Query.
            receiver: An instance or None supporting the method set_result_value,
                receiver.set_result_value() will be called once the Query has terminated.
        Returns:
            A Deferred instance if the Query is async, None otherwise
        """

        super(Forwarder, self).forward(query, deferred, execute, user)

        # We suppose we have no namespace from here
        qp = QueryPlan()
        qp.build_simple(query, self.metadata, self.allowed_capabilities)
        self.init_from_nodes(qp, user)

        d = defer.Deferred() if is_deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d, receiver)
