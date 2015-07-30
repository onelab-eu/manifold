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
from manifold.core.query                import Query
from manifold.gateways.object           import ManifoldCollection

from manifold.util.log                  import Log
from manifold.util.predicate            import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains
from manifold.util.type                 import accepts, returns

class WikipediaCollection(ManifoldCollection):

    def __init__(self, object_name, config):
        Log.tmp(object_name,config)
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
            field_names, field_types = list(), list()
            Log.tmp(self._config['fields'])
            for name, type in self._config["fields"]:
                field_names.append(name)
                field_types.append(type)
        except Exception, e:
            import traceback
            traceback.print_exc()
            Log.warning("Wrong format for fields in platform configuration: %s" %e)

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
                    is_array    = False, #(type.data_type == "ARRAY"),
                    description = self.get_description(name)
                )
                fields[name] = field
        Log.tmp(fields)
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

            #qualifiers = []            # No Qualifiers
            #qualifiers = ["local"]    # local
            qualifiers  = ["const"]   # Const means readonly fields

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
            raise RuntimeError("Missing key in platform configuration: %s" %e)

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
        capabilities.join     = True 
        capabilities.selection  = True
        capabilities.projection = True
   
        # Static data model
        return capabilities

    @staticmethod
    @returns(StringTypes)
    def to_wikipedia_where(predicates):
        """
        Translate a set of Predicate instances in the corresponding SQL string
        Args:
            predicates: A set of Predicate instances (list, set, frozenset, generator, ...)
        Returns:
            A String containing the corresponding SQL WHERE clause.
            This String is equal to "" if filters is empty
        """
        # NOTE : How to handle complex clauses
        return "|".join(predicates)

    # TODO: cache the result to avoid multiple calls
    def get_wikipedia_article(self, object_name, title):
        # ---------------------------
        # Requirements
        # ---------------------------
        # pip install kitchen simplemediawiki
        # ---------------------------
        description = {}
        try:
            from simplemediawiki import MediaWiki
            wiki = MediaWiki('https://en.wikipedia.org/w/api.php')
            description = wiki.call({
                'action': 'query', 
                'prop': 'extracts', 
                'titles': title,
                'format': 'json',
                'exintro': 'explaintext',
                'exsectionformat': 'plain',
                })
            # 'https://en.wikipedia.org/w/api.php?action=query&prop=extracts&format=json&exintro=&explaintext=&exsectionformat=plain&titles=' + titles
        except Exception, e:
            import traceback
            traceback.print_exc()
            Log.error("Error while getting the article from wikipedia")

        return description

    def get(self, packet):
        #  this example will just send an empty list of Records
        try:
            records = list()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Get data records from your platform

            # packet is used if the GW supports filters and fields selection
            data = packet.get_data()
            destination = packet.get_destination()
            object_name = destination.get_object_name()

            query = Query.from_packet(packet)
            where = query.get_where()
            values = list()
            record = {}
            if where is not None:
                for predicate in where:
                    if predicate.get_op() is eq:
                        values.append(predicate.get_value())

                        for k in self.get_key():
                            if k == predicate.get_key():
                                record[k]=predicate.get_value()

                description = self.get_wikipedia_article(object_name, " ".join(values))

                record['wikipedia_'+object_name] = description

                records.append(record)
                #where = WikipediaCollection.to_wikipedia_where(query.get_where())
            # send the records
            self.get_gateway().records(records, packet)

        except Exception as e:
            import traceback
            traceback.print_exc()
            raise Exception("Error in WikipediaCollection on get() function: %s" % e)
