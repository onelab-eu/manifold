#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with perfSONAR
# http://www.perfsonar.net/
#
# Adriano Spinola   <adriano.spinola@gmail.com>
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UFPE/UPMC 

from manifold.core.field                import Field
from manifold.core.table                import Table
from manifold.core.result_value         import ResultValue
from manifold.core.announce             import Announce, announces_from_docstring
from manifold.operators                 import LAST_RECORD
from manifold.gateways.gateway          import Gateway
from manifold.util.log                  import Log

class PerfSONARGateway(Gateway):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, interface, platform_name, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the Platform related to this
                Gateway or None.
            platform_config: A dictionnary containing the configuration related
                to the Platform managed by this Gateway. In practice, it should
                correspond to the following value stored in the Storage verifying
                
                    SELECT config FROM local:platform WHERE platform == "platform_name"
        """
        super(PerfSONARGateway, self).__init__(interface, platform_name, platform_config)

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

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, account_config = None, format = "dict", receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        super(FooGateway, self).forward(query, callback, is_deferred, execute, user, account_config, format, receiver)
        identifier = receiver.get_identifier() if receiver else None
        Log.tmp("Received: %s" % query)

        # Results of the query (TODO)
        rows = list() 

        # Adding a flag indicating this is the last record
        rows.append(LAST_RECORD)

        # Sending rows to parent processing node in the AST
        for row in rows:
            self.send(row, callback, identifier)

        self.success(receiver, query)

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


