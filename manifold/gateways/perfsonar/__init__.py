#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with perfSONAR
#
# Adriano Spinola   <adriano.spinola@gmail.com>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UFPE/UPMC 

from manifold.gateways      import Gateway
from manifold.core.record   import Record, Records, LastRecord
from manifold.core.table    import Table
from manifold.core.field    import Field
from manifold.core.announce import Announce, announces_from_docstring
from manifold.util.log      import Log

class PerfSONARGateway(Gateway):
    __gateway_name__ = 'perfsonar'

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

        Log.tmp("Received: %s" % self.query)

        # Results of the query (TODO)
        rows = []

        # Sending rows to parent processing node in the AST
        map(self.send, Records(rows))
        self.send(LastRecord())

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @announces_from_docstring('perfsonar')
    def get_metadata(self):
        """
        class dummy {
            int key;
            string key_value;
            CAPABILITY(selection,projection,retrieve,join);
            KEY(key);
        }; 
        """


