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

from types                              import StringTypes

from manifold.core.capabilities         import Capabilities
from manifold.core.object               import ObjectFactory
from manifold.core.field                import Field
from manifold.core.key                  import Key
from manifold.core.keys                 import Keys

from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldCollection

from manifold.util.log                  import Log
from manifold.util.type                 import accepts, returns

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

class MyCollection(ManifoldCollection):

    def __init__(self, object_name, config):
        if not isinstance(config, dict):
            raise Exception("Wrong format for field description. Expected dict")

        self._config = config

        # Static data model
        # Get fields and key from config of the platform 
        if 'fields' in config and 'key' in config:
            field_names, field_types = self.get_fields_from_config()
            self._field_names = field_names
            self._field_types = field_types
        # Dynamic data model
        # fields not specified must be discovered by the GW in make_object function
        else:
            self._field_names = None
            self._field_types = None

        self._cls = self.make_object(object_name, config)

    def get_fields_from_config(self):
        try:
            field_names, field_types = None, None
            for name, type in self._config["fields"]:
                field_names.append(name)
                field_types.append(type)
        except Exception, e:
            Log.warning("Wrong format for fields in platform configuration")

        return (field_names, field_types)

    def make_object(self, object_name, options):

        fields = dict()

        # Static data model
        if self._field_names and self._field_types:
            for name, type in zip(self._field_names, self._field_types):
                field = Field(
                    type        = type,
                    name        = name,
                    qualifiers  = self.get_qualifiers(name),
                    is_array    = (type.data_type == "ARRAY"),
                    description = self.get_description(name)
                )
                fields[name] = field

        keys = Keys()
        key_field_names = self.get_key()
        if key_field_names:
            if isinstance(key_field_names, StringTypes):
                key_fields = frozenset([fields[key_field_names]])
            elif isinstance(key_field_names, (list, set, frozenset, tuple)):
                key_fields = frozenset([fields[key_elt] for key_elt in key_field_names])
            keys.add(Key(key_fields))

        obj = ObjectFactory(object_name)
        obj.set_fields(fields.values())
        obj.set_keys(keys)
        obj.set_capabilities(self.get_capabilities())
        
        return obj

    @returns(list)
    def get_qualifiers(self, field_name):
        """
        Get the qualifiers of a field 
        Args:
            field_name: name of the field

        Static data model
            return []

        Dynamic data model
            get the qualifiers of a field from the platform

        Returns:
            A list.
        """
        try:
            # Static data model
            # Get the qualifiers of the field from the platform_config

            qualifiers = []            # No Qualifiers
            #qualifiers = ["local"]    # local
            #qualifiers  = ["const"]   # Const means readonly fields

            # Dynamic data model
            # Get the qualifiers of the field from the platform
            # -----------------------------
            #      Add your code here 
            # -----------------------------
 
        except Exception as e:
            Log.warning("Missing qualifiers for field: %s" % field_name)

        return qualifiers

    @returns(StringTypes)
    def get_description(self, field_name):
        """
        Get the description of a field 
        Args:
            field_name: name of the field

        Static data model
            return (null)

        Dynamic data model
            get the description of a field from the platform

        Returns:
            A String (the description of a field involved in the Key).
        """
        # NOTE only a single key is supported
        try:
            # Static data model
            # Get the description of the field from the platform_config
            return '(null)'

            # Dynamic data model
            # Get the description of the field from the platform
            # -----------------------------
            #      Add your code here 
            # -----------------------------
 
        except Exception as e:
            Log.warning("Missing descritpion for field: %s" % field_name)

    @returns(list)
    def get_key(self):
        """
        Extract the Fields involved in the key 
        
        Static data model
            specified in the platform.config

        Dynamic data model
            get the key from the platform

        Returns:
            A list of Strings (the fields involved in the Key).
        """
        # NOTE only a single key is supported
        try:
            # Static data model
            # Get the key from the platform_config
            if "key" in self._config:
                key = self._config["key"].split(",")
            # Dynamic data model
            # Get the key from the platform
            else:
                # -----------------------------
                #      Add your code here 
                # -----------------------------
                log.tmp("add your code here")

        except Exception as e:
            raise RuntimeError("Missing key in platform configuration")

        return key

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Extract the Capabilities from platform.config
        Returns:
            The corresponding Capabilities instance.
        """
        capabilities = Capabilities()

        # Default capabilities if they are not retrieved
        capabilities.retrieve = True
        capabilities.join     = False # Since we cannot filter by key, the join Capabilities must be disabled.

        # Static data model
        # Get the capabilities from the platform_config
        if "capabilities" in self._config:
            capabilities_str = self._config["capabilities"].split(",")
            for capability_str in capabilities_str:
                setattr(capabilities, capability_str, True)

        # Dynamic data model
        # Get the capabilities from the platform
        else:
            # -----------------------------
            #      Add your code here 
            # -----------------------------
            log.tmp("add your code here")

        return capabilities

    def get(self, packet):
        #  this example will just send an empty list of Records
        try:
            records = list()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Get data records from your platform

            # packet is used if the GW supports filters and fields selection

            # send the records
            self.get_gateway().records(records, packet)

        except Exception as e:
            raise Exception("Error in MyGateway on get() function: %s" % e)

    def create(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Create a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def update(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Update a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def delete(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Delete a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def execute(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Execute a method on your platform

        if packet.is_last():
            Log.info("Last record")
