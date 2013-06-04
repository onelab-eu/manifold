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
    # builds the query plan, instanciate the gateways and execute query plan using deferred if required
    def forward(self, query, deferred = False, execute = True, user = None):
        """
        Process a query.
        Args:
            query: A Query instance
            deferred: A boolean
            execute: A boolean set to True if the query must be processed
            user:
        Returns:
            A ResultValue instance containing the requested records (if any)
            the error message (if any) and so on.
        """
        super(Forwarder, self).forward(query, deferred, execute, user)

        # We suppose we have no namespace from here
        qp = QueryPlan()
        qp.build_simple(query, self.metadata, self.allowed_capabilities)
        self.instanciate_gateways(qp, user, query.get_timestamp())
        d = defer.Deferred() if is_deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d)
