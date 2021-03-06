#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with the agent table 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2012-2013 UPMC 
#

from copy                           import copy
from types                          import StringTypes
from manifold.gateways.postgresql   import PostgreSQLGateway
from manifold.util.type             import accepts, returns 

#-----------------------------------------------------------------------
# Type related to a set of view_agents records
#-----------------------------------------------------------------------

class Agent(list):
    @returns(dict)
    def repack(self, query, agent):
        """
        Repack an Agent record (dict) according to the issued Query

        Args:
            query: The Query instance handled by Manifold
            agent: A dictionnary corresponding to a fetched Agent record 
        """
        if "platform" in query.get_select():
            agent["platform"] = "tdmi"

    @returns(bool)
    def need_repack(self, query):
        return "platform" in query.get_select()

    def __init__(self, query, db = None):
        """
        Constructor
        Args:
            query: the query to be executed.
                query.get_select():
                    An array which contains the fields we want to retrieve
                query.get_timestamp():
                    You may also pass a tuple (ts_min, ts_max) or a list [ts_min, ts_max].
                    In this case the function fetch records which were active at [t1, t2]
                    such that [t1, t2] n [ts_min, ts_max] != emptyset.
                    In this syntax, ts_min and ts_max might be equal to None if unbounded.
            db: Pass a reference to a database instance
        """
        self.db = db
        self.query = query

    def get_sql(self):
        select_bak = copy(self.query.get_select())
        from_bak   = self.query.get_from()

        # Tweak SELECT 
        if "platform" in self.query.get_select():
            self.query.fields.remove("platform")

        # Craft the SQL query
        sql = PostgreSQLGateway.to_sql(self.query)

        # Fix FROM and SELECT
        self.query.fields = select_bak
        self.query.object = from_bak
        
        return sql
