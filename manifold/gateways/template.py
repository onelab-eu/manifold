#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Short description  
#
# Firstname Lastname        <firstname.lastname@organisation>
#
# Copyright (C) 2013

# Add here standard required python modules (TODO)

# Add required Manifold modules in the following list (TODO)

from manifold.core.announce import Announce
from manifold.core.field    import Field
from manifold.core.record   import Record, Records, LastRecord
from manifold.core.table    import Table
from manifold.gateways      import Gateway
from manifold.util.log      import Log
from manifold.util.type     import accepts, returns

class FooGateway(Gateway):
    # You may inherits another Gateway, for instance a PostgreSQLGateway.
    # If so, import the appropriate Manifold module.
    #
    # See also:
    #    manifold/gateways/*

    __gateway_name__ = 'template'

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
        super(FooGateway, self).__init__(interface, platform_name, platform_config)

    #---------------------------------------------------------------------------
    # Accessors 
    #---------------------------------------------------------------------------

    # (TODO)

    #---------------------------------------------------------------------------
    # Connection 
    #---------------------------------------------------------------------------

    # (TODO)

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

        # Handle the query and feed rows with dictionary having the key corresponding
        # to query.get_select() and containing value satisfying query.get_where().
        #
        # See also:
        #   manifold/core/query.py

        # Results of the query (TODO)
        rows = Records() 

        # Sending rows to parent processing node in the AST
        for row in rows:
            self.send(row, callback, identifier)

        self.send(LastRecord())
        self.success(receiver, query)

        ## In case of failure, you would return something like this:
        # self.error(receiver, query, message)

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @returns(list)
    def get_metadata(self):
        """
        Build metadata by querying postgresql's information schema
        Returns:
            The list of corresponding Announce instances
        """
        announces = list()
        
        # Feed announces by adding Announce instances representing the Table
        # provided by this Gateway (TODO). An Announce embeds a Table instance
        # which stores a set of Fields and Keys.
        #
        # See also:
        #    manifold/core/announce.py
        #    manifold/core/field.py
        #    manifold/core/key.py
        #    manifold/core/table.py

        return announces 
