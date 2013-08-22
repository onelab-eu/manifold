#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with perfSONAR
#
# Adriano Spinola   <adriano.spinola@gmail.com>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UFPE/UPMC 

from manifold.gateways                  import Gateway, LAST_RECORD
from manifold.core.table                import Table
from manifold.core.field                import Field
from manifold.core.announce             import Announce
from manifold.util.log                  import Log

class PerfSONARGateway(Gateway):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform, query, config, user_config, user):
        """
        Construct a PerfSONARGateway instance
        """
        super(PerfSONARGateway, self).__init__(router, platform, query, config, user_config, user)

        # Other initialization here

    #---------------------------------------------------------------------------
    # Accessors 
    #---------------------------------------------------------------------------

    # TODO

    #---------------------------------------------------------------------------
    # Connection 
    #---------------------------------------------------------------------------

    # TODO

    #---------------------------------------------------------------------------
    # Overloaded methods 
    #---------------------------------------------------------------------------

    def forward(self, query, deferred = False, execute = True, user = None):
        """
        Args
            query: A Query instance that must be processed by this PostgreSQLGateway
            deferred: A boolean
            execute: A boolean which must be set to True if the query must be run.
            user: A User instance or None
        """
        self.query = query
        self.start()

    def start(self):
        """
        Fetch records stored in the postgresql database according to self.query
        """

        # Results of the query (TODO)
        rows = []

        # Adding a flag indicating this is the last record
        rows.append(LAST_RECORD)

        # Sending rows to parent processing node in the AST
        map(self.send, rows)

        return 
       

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    def get_metadata(self):
        """
        Build metadata by querying postgresql's information schema
        Returns:
            The list of corresponding Announce instances
        """
        return [] 
