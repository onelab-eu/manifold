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

from manifold.core.announce import Announce, announces_from_docstring
from manifold.core.table    import Table
from manifold.gateways      import Gateway
from manifold.util.log      import Log
from manifold.util.type     import accepts, returns

class FooGateway(Gateway): # (TODO) Update this class name
    # You may inherits another Gateway, for instance a PostgreSQLGateway.
    # If so, import the appropriate Manifold module.
    #
    # See also:
    #    manifold/gateways/*

    __gateway_name__ = 'foo' # (TODO) Update this String (See gateway.gateway_type in Manifold Storage)

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

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Remark:
            An Exception raised in this method will be translated into
            the corresponding ErrorPacket instance.
            See Gateway::receive()
        Args:
            packet: A QUERY Packet instance.
        """
        # QUERY Packets carry information such as the incoming Query,
        # and some additionnal Annotations carrying (for instance the user
        # credentials).
        query = packet.get_query()
        records = list() 

        # Fill records by appending Record or dict instances
        # according to "query"
        records.append({
            "field1" : "value1",
            "field2" : "value2"
        })

        # - See Gateway::records()
        # - See Gateway::warning()
        # - See Gateway::error()
        self.records(records, packet)

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @returns(list)
    def make_announces(self):
        """
        Build announces by querying postgresql's information schema
        Returns:
            The list of corresponding Announce instances
        """
        announces = list()
        
        # (TODO)
        #
        # Feed announces by adding Announce instances representing the Table
        # provided by this Gateway. An Announce embeds a Table instance
        # which stores a set of Fields and Keys.
        #
        # You should define FooGateway's metadata by using:
        #
        #    @announces_from_docstring('foo')
        #
        # ... as illustrated in manifold.util.storage and in in metadata/*.h
        #
        # Otherwise you can also manually craft the Announce(s) instances.
        #   See manifold/core/announce.py

        return announces 
