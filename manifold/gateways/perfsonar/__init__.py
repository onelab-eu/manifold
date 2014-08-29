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
from manifold.core.record               import Record, Records
from manifold.core.table                import Table
from manifold.core.announce             import Announce, announces_from_docstring
from manifold.gateways                  import Gateway
from manifold.util.log                  import Log

class PerfSONARGateway(Gateway):
    __gateway_name__ = "perfsonar"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform_name, platform_config = None):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform_name: A String storing name of the Platform related to this
                Gateway or None.
            platform_config: A dictionnary containing the configuration related
                to the Platform managed by this Gateway. In practice, it should
                correspond to the following value stored in the Storage verifying
                
                    SELECT config FROM local:platform WHERE platform == "platform_name"
        """
        super(PerfSONARGateway, self).__init__(router, platform_name, platform_config)

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

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
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

    @announces_from_docstring("perfsonar")
    def make_announces(self):
        """
        class dummy {
            int    key;       /**< My key */
            string key_value; /**< My value */

            CAPABILITY(selection,projection,retrieve,join);
            KEY(key);
        }; 
        """
