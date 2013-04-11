#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Query representation
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>


class LocalRouter(object):
    """
    Implements an abstraction of a Router.
    """

    LOCAL_NAMESPACE = "tophat"

    _map_local_table = {
        "platform" : Platform,
        "user"     : User,
        "account"  : Account
    }

class FlowAwareRouter(LocalRouter):

    def __init__(self):
        self.flow_table = FlowTable()

    def get_query_plan(self, packet):

        # Get flow from packet

        try:
            query_plan = self.flowtable[flow]

        # TODO change exception name
        except KeyError, flow:
            # Compute query_plan from route in FIB
            query_plan = None

            try:
                route = self.fib[destination]

            except KeyError, destination:
            
                # Compute route from routes in RIB
                route = None

                # Insert route in FIB
                self.fib.add(route)
            
            # We have the route, compute a query plane
            query_plan = None

        
class Router(LocalRouter):
    def boot(self):
        super(Router, self).boot()

        # Establish session towards peers for dynamic route update (if needed)
        # TODO

        # Listen for queries (if needed)
        # TODO
        
