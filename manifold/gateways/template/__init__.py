#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Example of a Gateway
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loïc Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2015 UPMC

from manifold.gateways                      import Gateway
from manifold.gateways.template.collection  import MyCollection
from manifold.util.log                      import Log
from manifold.util.type                     import accepts, returns

class MyGateway(Gateway):
    # this gateway_name must be used as gateway_type when adding a platform to the local storage 
    __gateway_name__ = "my_gateway"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    # XXX Don't we use .h files anymore?

    def __init__(self, router, platform, **platform_config):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.

                -------------------------------------------------------------
                1 - Static data model 
                -------------------------------------------------------------
                Fields, types and key are defined in platform_config
                In pratice this dictionnary is built as follows:

                    {
                        "table_name" : {
                            "fields"   : [
                                ["field_name1", "type1"],
                                ...
                            ],
                            "key" : "field_name_i, field_name_j, ..."
                        },
                        ...
                    }

                -------------------------------------------------------------
                2 - Dynamic data model 
                -------------------------------------------------------------
                Fields, types and key have to be discovered from the platform

        """
        super(MyGateway, self).__init__(router, platform, **platform_config)

        objects = self.get_objects()
        for object_name, config in objects:
            collection = MyCollection(object_name, config)
            self.register_collection(collection)

    @returns(list)
    def get_objects(self, platform_config):
        """
        Get the list of objects advertised by the platform 
        Args:
            platform_config

        Static data model
            returns the list of objects stored in local platform_config
            [(object_name,{config}),(object_name,{config})]

        Dynamic data model
            get the list of objects from the platform

        Returns:
            A list of objects.
        """

        # Dynamic data model
        # -----------------------------
        #      Add your code here 
        # -----------------------------

        # Format should be as follows: [(object_name,{config}),(object_name,{config})]

        # Static data model
        return platform_config.items() 


